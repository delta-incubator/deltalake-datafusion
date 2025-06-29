use std::sync::Arc;

use datafusion_catalog::Session;
use datafusion_common::{
    error::{DataFusionError, DataFusionErrorBuilder, Result},
    exec_datafusion_err as exec_err,
};
use delta_kernel::{Engine, ExpressionRef, Predicate};
use delta_kernel::{Version, arrow::datatypes::SchemaRef as ArrowSchemaRef};
use delta_kernel::{
    actions::Metadata,
    scan::state::{DvInfo, Stats},
};
use delta_kernel::{
    actions::Protocol,
    schema::{Schema as DeltaSchema, SchemaRef as DeltaSchemaRef},
};
use delta_kernel::{engine::arrow_conversion::TryIntoArrow, snapshot::Snapshot};
use url::Url;

use super::table_format::{ScanFileContext, TableScan, TableSnapshot};
use crate::error::to_df_err;
use crate::session::KernelSessionExt;

pub struct DeltaTableSnapshot {
    snapshot: Arc<Snapshot>,
    table_schema: ArrowSchemaRef,
}

impl std::fmt::Debug for DeltaTableSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableSnapshot")
            .field("snapshot", &self.snapshot)
            .field("table_schema", &self.table_schema)
            .finish()
    }
}

impl DeltaTableSnapshot {
    pub fn try_new(snapshot: Arc<Snapshot>) -> Result<Self> {
        let table_schema = Arc::new(snapshot.schema().as_ref().try_into_arrow()?);
        Ok(Self {
            snapshot,
            table_schema,
        })
    }
}

#[async_trait::async_trait]
impl TableSnapshot for DeltaTableSnapshot {
    fn table_schema(&self) -> &ArrowSchemaRef {
        &self.table_schema
    }

    fn schema(&self) -> DeltaSchemaRef {
        self.snapshot.schema()
    }

    fn version(&self) -> Version {
        self.snapshot.version()
    }

    fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Predicate>,
    ) -> Result<TableScan> {
        scan_metadata(
            state.kernel_engine()?,
            &self.snapshot,
            projection,
            predicate,
            &self.table_schema,
        )
        .await
    }
}

async fn scan_metadata(
    engine: Arc<dyn Engine>,
    snapshot: &Arc<Snapshot>,
    projection: Option<&Vec<usize>>,
    predicate: Arc<Predicate>,
    table_schema: &ArrowSchemaRef,
) -> Result<TableScan> {
    let projected_delta_schema = project_delta_schema(table_schema, snapshot.schema(), projection);

    let scan = snapshot
        .clone()
        .scan_builder()
        .with_schema(projected_delta_schema)
        .with_predicate(predicate)
        .build()
        .map_err(to_df_err)?;

    let table_root = scan.table_root().clone();
    let physical_schema: ArrowSchemaRef =
        Arc::new(scan.physical_schema().as_ref().try_into_arrow()?);
    let physical_predicate = scan.physical_predicate();
    let logical_schema = Arc::new(scan.logical_schema().as_ref().try_into_arrow()?);

    let scan_inner = move || {
        let mut context = ScanContext::new(engine.clone(), table_root);
        let meta = scan.scan_metadata(engine.as_ref()).map_err(to_df_err)?;
        for scan_meta in meta {
            let scan_meta = scan_meta.map_err(to_df_err)?;
            context = scan_meta
                .visit_scan_files(context, visit_scan_file)
                .map_err(to_df_err)?;
        }
        context.errs.error_or(context.files)
    };
    let files = tokio::task::spawn_blocking(scan_inner)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))??;

    Ok(TableScan {
        files,
        physical_schema,
        physical_predicate,
        logical_schema,
    })
}

struct ScanContext {
    /// Kernel engine for reading deletion vectors.
    engine: Arc<dyn Engine>,
    /// Table root URL
    table_root: Url,
    /// Files to be scanned.
    files: Vec<ScanFileContext>,
    /// Errors encountered during the scan.
    errs: DataFusionErrorBuilder,
}

impl ScanContext {
    fn new(engine: Arc<dyn Engine>, table_root: Url) -> Self {
        Self {
            engine,
            table_root,
            files: Vec::new(),
            errs: DataFusionErrorBuilder::new(),
        }
    }

    fn parse_path(&self, path: &str) -> Result<Url> {
        Ok(match Url::parse(path) {
            Ok(url) => url,
            Err(_) => self
                .table_root
                .join(path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        })
    }
}

fn visit_scan_file(
    ctx: &mut ScanContext,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    // NB: partition values are passed for backwards compatibility
    // all required transformations are now part of the transform field
    _: std::collections::HashMap<String, String>,
) {
    let file_url = match ctx.parse_path(path) {
        Ok(v) => v,
        Err(e) => {
            ctx.errs.add_error(e);
            return;
        }
    };

    // Get the selection vector (i.e. inverse deletion vector)
    let Ok(selection_vector) = dv_info.get_selection_vector(ctx.engine.as_ref(), &ctx.table_root)
    else {
        ctx.errs
            .add_error(exec_err!("Failed to get selection vector"));
        return;
    };

    ctx.files.push(ScanFileContext {
        selection_vector,
        transform,
        stats,
        file_url,
        size: size as u64,
    });
}

fn project_delta_schema(
    arrow_schema: &ArrowSchemaRef,
    schema: DeltaSchemaRef,
    projections: Option<&Vec<usize>>,
) -> DeltaSchemaRef {
    if let Some(projections) = projections {
        let projected_fields = projections
            .iter()
            .filter_map(|i| schema.field(arrow_schema.field(*i).name()))
            .cloned();
        Arc::new(DeltaSchema::new(projected_fields))
    } else {
        schema
    }
}
