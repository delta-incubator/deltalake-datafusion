//! Datafusion `TableProvider` implementation for Delta tables.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetAccessPlan, RowGroupAccess,
};
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::parquet::arrow::arrow_reader::RowSelection;
use datafusion::parquet::file::metadata::RowGroupMetaData;
use datafusion::physical_plan::{ExecutionPlan, union::UnionExec};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::error::Result;
use datafusion_common::{DFSchema, DataFusionError, HashMap, ScalarValue, not_impl_err};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::PhysicalExpr;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use delta_kernel::ExpressionRef;
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::schema::DataType as DeltaDataType;
use delta_kernel::snapshot::Snapshot;
use futures::stream::{StreamExt, TryStreamExt};
use itertools::Itertools;

use self::exec::{DeltaScanExec, FILE_ID_FIELD};
pub use self::snapshot::DeltaTableSnapshot;
pub use self::table_format::{ScanFileContext, TableScan, TableSnapshot};
use crate::engine::NestedSchemaAdapterFactory;
use crate::engine::{to_datafusion_expr, to_delta_predicate};
use crate::session::KernelSessionExt as _;
use crate::utils::{AsObjectStorePath, AsObjectStoreUrl};

mod exec;
mod snapshot;
mod table_format;

pub struct DeltaTableProvider {
    snapshot: Arc<DeltaTableSnapshot>,
    pq_source: Arc<ParquetSource>,
}

impl std::fmt::Debug for DeltaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableProvider")
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

impl DeltaTableProvider {
    pub fn try_new(snapshot: Arc<Snapshot>) -> Result<Self> {
        let snapshot = DeltaTableSnapshot::try_new(snapshot)?;
        let parquet_source = ParquetSource::default()
            .with_schema_adapter_factory(Arc::new(NestedSchemaAdapterFactory));
        Ok(Self {
            snapshot: Arc::new(snapshot),
            pq_source: Arc::new(parquet_source),
        })
    }

    pub(crate) fn current_snapshot(&self) -> &Arc<Snapshot> {
        &self.snapshot.current_snapshot()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(self.snapshot.table_schema())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pred = to_delta_predicate(filters)?.into();
        let table_scan = self.snapshot.scan_metadata(state, projection, pred).await?;

        let delta_schema = self.snapshot.schema().into();

        // Convert the delta expressions from the scan into a map of file id to datafusion physical expression
        // these will be applied to convert the raw data read from disk into the logical table schema
        let physical_schema_df = table_scan.physical_schema.clone().try_into()?;
        let file_transform = |file_ctx: &ScanFileContext| {
            file_ctx.transform.as_ref().map(|t| {
                to_physical(state, &physical_schema_df, t, &delta_schema)
                    .map(|expr| (file_ctx.file_url.to_string(), expr))
            })
        };
        let transform_by_file = table_scan
            .files
            .iter()
            .filter_map(file_transform)
            .try_collect::<_, HashMap<_, _>, _>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Convert the files into datafusions `PartitionedFile`s grouped by the object store they are stored in
        // this is used to create a DataSourceExec plan for each store
        // To correlate the data with the original file, we add the file url as a partition value
        // This is required to apply the correct transform to the data in downstream processing.
        let to_partitioned_file = |f: ScanFileContext| {
            let file_path = f.file_url.as_object_store_path();
            let mut partitioned_file = PartitionedFile::new(file_path.to_string(), f.size);
            partitioned_file.partition_values =
                vec![ScalarValue::Utf8(Some(f.file_url.to_string()))];
            // NB: we need to reassign the location since the 'new' method does
            // incorrect or inconsistent encoding internally.
            partitioned_file.object_meta.location = file_path;
            Ok::<_, DataFusionError>((
                f.file_url.as_object_store_url(),
                (partitioned_file, f.selection_vector),
            ))
        };

        let files_by_store = table_scan
            .files
            .into_iter()
            .flat_map(to_partitioned_file)
            .into_group_map();

        let plan = get_read_plan(
            files_by_store,
            &table_scan.physical_schema,
            state,
            limit,
            self.pq_source.as_ref().clone(),
        )
        .await?;

        Ok(Arc::new(DeltaScanExec::new(
            table_scan.logical_schema,
            plan,
            Arc::new(transform_by_file),
        )))
    }

    /// Return an [`ExecutionPlan`] to insert data into this table, if
    /// supported.
    ///
    /// The returned plan should return a single row in a UInt64
    /// column called "count" such as the following
    ///
    /// ```text
    /// +-------+,
    /// | count |,
    /// +-------+,
    /// | 6     |,
    /// +-------+,
    /// ```
    ///
    /// # See Also
    ///
    /// See [`DataSinkExec`] for the common pattern of inserting a
    /// streams of `RecordBatch`es as files to an ObjectStore.
    ///
    /// [`DataSinkExec`]: datafusion_datasource::sink::DataSinkExec
    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not implemented for this table")
    }
}

async fn get_read_plan(
    files_by_store: impl IntoIterator<
        Item = (ObjectStoreUrl, Vec<(PartitionedFile, Option<Vec<bool>>)>),
    >,
    physical_schema: &ArrowSchemaRef,
    state: &dyn Session,
    limit: Option<usize>,
    base_source: ParquetSource,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: update parquet source.
    let source = Arc::new(base_source);
    let metrics = ExecutionPlanMetricsSet::new();

    let mut plans = Vec::new();

    for (store_url, files) in files_by_store.into_iter() {
        state.ensure_object_store(store_url.as_ref()).await?;

        let store = state.runtime_env().object_store(&store_url)?;
        let reader_factory = source
            .parquet_file_reader_factory()
            .cloned()
            .unwrap_or_else(|| Arc::new(DefaultParquetFileReaderFactory::new(store)));

        let file_group = compute_parquet_access_plans(&reader_factory, files, &metrics).await?;

        // TODO: convert passed predicate to an expression in terms of physical columns
        // and add it to the FileScanConfig
        let config = FileScanConfigBuilder::new(store_url, physical_schema.clone(), source.clone())
            .with_file_group(file_group.into_iter().collect())
            .with_table_partition_cols(vec![FILE_ID_FIELD.clone()])
            .with_limit(limit)
            .build();
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        plans.push(plan);
    }

    let plan = match plans.len() {
        1 => plans.remove(0),
        _ => Arc::new(UnionExec::new(plans)),
    };
    Ok(match plan.with_fetch(limit) {
        Some(limit) => limit,
        None => plan,
    })
}

// convert a delta expression to a datafusion physical expression
// we return a vector of expressions implicitly representing structs,
// as there is no top-level Struct expression type in datafusion
fn to_physical(
    state: &dyn Session,
    physical_schema_df: &DFSchema,
    transform: &ExpressionRef,
    output_type: &DeltaDataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    state.create_physical_expr(
        to_datafusion_expr(transform, output_type)?,
        physical_schema_df,
    )
}

async fn compute_parquet_access_plans(
    reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    files: Vec<(PartitionedFile, Option<Vec<bool>>)>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<Vec<PartitionedFile>> {
    futures::stream::iter(files)
        // HACK: using filter_map here since 'map' somehow does not accept futures.
        .filter_map(|(partitioned_file, selection_vector)| async {
            if let Some(sv) = selection_vector {
                Some(pq_access_plan(reader_factory, partitioned_file, sv, metrics).await)
            } else {
                Some(Ok(partitioned_file))
            }
        })
        .try_collect::<Vec<_>>()
        .await
}

async fn pq_access_plan(
    reader_factory: &Arc<dyn ParquetFileReaderFactory>,
    partitioned_file: PartitionedFile,
    selection_vector: Vec<bool>,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<PartitionedFile> {
    let mut parquet_file_reader = reader_factory.create_reader(
        0,
        partitioned_file.object_meta.clone().into(),
        None,
        metrics,
    )?;

    let parquet_metadata = parquet_file_reader.get_metadata(None).await?;
    let total_rows = parquet_metadata
        .row_groups()
        .iter()
        .map(RowGroupMetaData::num_rows)
        .sum::<i64>();

    let selection_vector = get_full_selection_vector(&selection_vector, total_rows as usize);

    // Create a ParquetAccessPlan that will be used to skip rows based on the selection vector
    let mut row_groups: Vec<RowGroupAccess> = vec![];
    let mut row_group_row_start = 0;
    for row_group in parquet_metadata.row_groups().iter() {
        // If all rows in the row group are deleted, skip the row group
        let row_group_access = get_row_group_access(
            &selection_vector,
            row_group_row_start,
            row_group.num_rows() as usize,
        );
        row_groups.push(row_group_access);
        row_group_row_start += row_group.num_rows() as usize;
    }

    let plan = ParquetAccessPlan::new(row_groups);

    Ok(partitioned_file.with_extensions(Arc::new(plan)))
}

fn get_row_group_access(selection_vector: &[bool], start: usize, offset: usize) -> RowGroupAccess {
    // If all rows in the row group are deleted (i.e. not selected), skip the row group
    if !selection_vector[start..start + offset].iter().any(|&x| x) {
        return RowGroupAccess::Skip;
    }
    // If all rows in the row group are present (i.e. selected), scan the full row group
    if selection_vector[start..start + offset].iter().all(|&x| x) {
        return RowGroupAccess::Scan;
    }

    let mask = selection_vector[start..start + offset].to_vec();

    // If some rows are deleted, get a row selection that skips the deleted rows
    let row_selection = RowSelection::from_filters(&[mask.into()]);
    RowGroupAccess::Selection(row_selection)
}

fn get_full_selection_vector(selection_vector: &[bool], total_rows: usize) -> Vec<bool> {
    let mut new_selection_vector = vec![true; total_rows];
    let copy_len = std::cmp::min(selection_vector.len(), total_rows);
    new_selection_vector[..copy_len].copy_from_slice(&selection_vector[..copy_len]);
    new_selection_vector
}
