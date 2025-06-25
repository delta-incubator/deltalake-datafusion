use std::sync::{Arc, mpsc};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, JsonSource, ParquetSource};
use datafusion_catalog::memory::DataSourceExec;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_datasource::PartitionedFile;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_plan::execute_stream;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_session::{Session, SessionStore};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::engine::{parse_json as arrow_parse_json, to_json_bytes};
use delta_kernel::object_store::{PutMode, path::Path};
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, EngineData, Error as DeltaError, Expression, ExpressionRef,
    FileDataReadResultIterator, FileMeta, JsonHandler, ParquetHandler,
};
use delta_kernel::{PredicateRef, object_store};
use futures::TryStreamExt;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::RwLock;
use tracing::warn;
use url::Url;

use super::expressions::to_datafusion_expr;
use super::schema_adapter::NestedSchemaAdapterFactory;
use crate::utils::{AsObjectStoreUrl, grouped_partitioned_files};

const DEFAULT_BUFFER_SIZE: usize = 1024;

pub struct DataFusionFileFormatHandler<E: TaskExecutor> {
    /// Shared session state for the session
    state: Arc<SessionStore>,
    /// The executor to run async tasks on
    task_executor: Arc<E>,
    /// size of the buffer (via our `sync_channel`).
    buffer_size: usize,
    /// Static configuration for reading json files
    json_source: Arc<JsonSource>,
    /// Static configuration for reading parquet files.
    parquet_source: Arc<ParquetSource>,
}

impl<E: TaskExecutor> std::fmt::Debug for DataFusionFileFormatHandler<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionFileFormatHandler")
            .field("state", &self.state)
            .field("buffer_size", &self.buffer_size)
            .finish()
    }
}

impl<E: TaskExecutor> DataFusionFileFormatHandler<E> {
    pub fn new(task_executor: Arc<E>, state: impl Into<Arc<SessionStore>>) -> Self {
        // TODO: evaluate further config options like more advanced pruning etc ...
        let parquet_source = ParquetSource::default()
            .with_schema_adapter_factory(Arc::new(NestedSchemaAdapterFactory));
        Self {
            state: state.into(),
            task_executor,
            buffer_size: DEFAULT_BUFFER_SIZE,
            json_source: Arc::new(JsonSource::default()),
            parquet_source: Arc::new(parquet_source),
        }
    }

    fn session(&self) -> DFResult<Arc<RwLock<dyn Session>>> {
        self.state
            .get_session()
            .upgrade()
            .ok_or_else(|| DataFusionError::Execution("no active session".into()))
    }

    fn physical_predicate(
        &self,
        predicate: &ExpressionRef,
        arrow_schema: &ArrowSchemaRef,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let df_schema = arrow_schema.clone().try_into().ok();
        let df_expr = to_datafusion_expr(predicate, &delta_kernel::schema::DataType::BOOLEAN).ok();
        if let (Some(df_expr), Some(df_schema)) = (df_expr, df_schema) {
            self.session()
                .ok()?
                .read()
                .create_physical_expr(df_expr, &df_schema)
                .ok()
        } else {
            None
        }
    }

    fn parquet_exec(
        &self,
        store_url: ObjectStoreUrl,
        files: Vec<PartitionedFile>,
        arrow_schema: ArrowSchemaRef,
        predicate: Option<&ExpressionRef>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut pq_source = self.parquet_source.as_ref().clone();
        if let Some(physical_predicate) =
            predicate.and_then(|p| self.physical_predicate(p, &arrow_schema))
        {
            pq_source = pq_source.with_predicate(arrow_schema.clone(), physical_predicate);
        }
        let config = FileScanConfigBuilder::new(store_url, arrow_schema, Arc::new(pq_source))
            .with_file_group(files.into_iter().collect())
            .build();
        // TODO: repartitition plan to read/parse from multiple threads
        DataSourceExec::from_data_source(config)
    }

    fn json_exec(
        &self,
        store_url: ObjectStoreUrl,
        files: Vec<PartitionedFile>,
        arrow_schema: ArrowSchemaRef,
    ) -> Arc<dyn ExecutionPlan> {
        let config = FileScanConfigBuilder::new(store_url, arrow_schema, self.json_source.clone())
            .with_file_group(files.into_iter().collect())
            .build();
        // TODO: repartitition plan to read/parse from multiple threads
        DataSourceExec::from_data_source(config)
    }

    fn get_plan(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        get_exec: impl Fn(
            ObjectStoreUrl,
            Vec<PartitionedFile>,
            ArrowSchemaRef,
        ) -> Arc<dyn ExecutionPlan>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        let files_by_store = grouped_partitioned_files(files)?;
        let arrow_schema: ArrowSchemaRef = Arc::new(physical_schema.as_ref().try_into_arrow()?);

        let mut plans = Vec::new();

        for (store_url, files) in files_by_store.into_iter() {
            plans.push(get_exec(store_url, files, arrow_schema.clone()));
        }

        Ok(match plans.len() {
            1 => plans.remove(0),
            _ => Arc::new(UnionExec::new(plans)),
        })
    }

    fn execute_plan(&self, plan: Arc<dyn ExecutionPlan>) -> FileDataReadResultIterator {
        let Ok(task_ctx) = self.session().map(|session| session.read().task_ctx()) else {
            return Box::new(std::iter::once(Err(DeltaError::Generic(
                "no active session".into(),
            ))));
        };

        let (tx, rx) = mpsc::sync_channel(self.buffer_size);

        // TODO: evaluate if we can re-use ReceiverStreamBuilder or related here
        self.task_executor.spawn(async move {
            let mut stream: BoxStream<'_, DeltaResult<Box<dyn EngineData>>> =
                match execute_stream(plan, task_ctx).map_err(DeltaError::generic_err) {
                    Ok(stream) => {
                        Box::pin(stream.map_err(DeltaError::generic_err).map_ok(|data| {
                            Box::new(ArrowEngineData::new(data)) as Box<dyn EngineData>
                        }))
                    }
                    Err(e) => {
                        warn!("failed to execute plan: {}", e);
                        Box::pin(stream::once(async move { Err(e) }))
                    }
                };

            // send each record batch over the channel
            while let Some(item) = stream.next().await {
                if tx.send(item).is_err() {
                    warn!("read_json receiver end of channel dropped before sending completed");
                }
            }
        });

        Box::new(rx.into_iter())
    }
}

impl<E: TaskExecutor> JsonHandler for DataFusionFileFormatHandler<E> {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        arrow_parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let get_exec =
            |store_url, files, arrow_schema| self.json_exec(store_url, files, arrow_schema);
        let plan = self.get_plan(files, physical_schema, get_exec)?;
        Ok(self.execute_plan(plan))
    }

    // note: for now we just buffer all the data and write it out all at once
    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        let buffer = to_json_bytes(data)?;
        let put_mode = if overwrite {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };

        let store_url = path.as_object_store_url();
        let store = self
            .session()
            .map_err(DeltaError::generic_err)?
            .read()
            .runtime_env()
            .object_store(store_url)
            .map_err(DeltaError::generic_err)?;

        let path = Path::from_url_path(path.path())?;
        let path_str = path.to_string();
        self.task_executor
            .block_on(async move { store.put_opts(&path, buffer.into(), put_mode.into()).await })
            .map_err(|e| match e {
                object_store::Error::AlreadyExists { .. } => {
                    DeltaError::FileAlreadyExists(path_str)
                }
                e => e.into(),
            })?;
        Ok(())
    }
}

impl<E: TaskExecutor> ParquetHandler for DataFusionFileFormatHandler<E> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let predicate =
            predicate.map(|p| Arc::new(Expression::Predicate(Box::new(p.as_ref().clone()))));
        let get_exec = |store_url, files, arrow_schema| {
            self.parquet_exec(store_url, files, arrow_schema, predicate.as_ref())
        };
        let plan = self.get_plan(files, physical_schema, get_exec)?;
        Ok(self.execute_plan(plan))
    }
}
