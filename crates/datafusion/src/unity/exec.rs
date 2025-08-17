use std::{any::Any, fmt, sync::Arc};

use arrow::array::RecordBatch;
use datafusion::{
    common::{DFSchemaRef, Statistics, internal_err},
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use unitycatalog_client::UnityCatalogClient;

use crate::KernelTaskContextExt;

#[async_trait::async_trait]
pub trait ExecutableUnityCatalogStement: std::fmt::Debug + Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn execute(&self, client: UnityCatalogClient) -> Result<RecordBatch>;
    fn return_schema(&self) -> &DFSchemaRef;
}

pub struct UnityCatalogRequestExec {
    request: Arc<dyn ExecutableUnityCatalogStement>,
    cache: PlanProperties,
}

impl UnityCatalogRequestExec {
    pub fn new(request: Arc<dyn ExecutableUnityCatalogStement>) -> Self {
        Self {
            cache: PlanProperties::new(
                EquivalenceProperties::new(Arc::new(request.return_schema().as_arrow().clone())),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            request,
        }
    }
}

impl std::fmt::Debug for UnityCatalogRequestExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("UnityCatalogRequestExec")
            .field("request", &self.request)
            .finish()
    }
}

impl DisplayAs for UnityCatalogRequestExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ExecuteUCStatement: statement={:?}", self.request)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for UnityCatalogRequestExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("CreateCatalogExec invalid partition {partition}");
        }

        let uc_client = context.kernel_ext()?.unity_catalog_client()?;
        let request = self.request.clone();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            Box::pin(futures::stream::once(async move {
                request.execute(uc_client).await
            })),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
