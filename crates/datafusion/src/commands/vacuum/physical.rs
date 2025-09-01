use std::{any::Any, fmt, sync::Arc};

use arrow::array::AsArray;
use datafusion::{
    common::{Result, Statistics},
    execution::{SendableRecordBatchStream, TaskContext, object_store::ObjectStoreUrl},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::ExecutionPlanMetricsSet,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt as _, TryStreamExt};
use object_store::path::Path;
use tracing::trace;

use super::logical::VACUUM_RETURN_SCHEMA;

/// VacuumExec executes a [VACUUM] operation against a given table.
///
/// [VACUUM]: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum
#[derive(Debug, Clone)]
pub struct VacuumExec {
    store_url: ObjectStoreUrl,
    /// Input plan that produces the files to be deleted.
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Properties, equivalence properties, partitioning, etc.
    cache: PlanProperties,
}

impl VacuumExec {
    pub fn new(store_url: ObjectStoreUrl, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            store_url,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: PlanProperties::new(
                EquivalenceProperties::new(VACUUM_RETURN_SCHEMA.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for VacuumExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "VacuumExec: inventory_plan={:?}", self.input)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for VacuumExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start VacuumExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let store = context.runtime_env().object_store(&self.store_url)?;

        let input_stream = self.input.execute(partition, context.clone())?;
        let input_stream = input_stream
            .map(move |b| (b, store.clone()))
            .then(|(maybe_batch, s)| async move {
                if let Ok(batch) = &maybe_batch {
                    let path_iter = batch
                        .column(0)
                        .as_string::<i32>()
                        .iter()
                        .filter_map(|s| s.map(Path::from))
                        .map(Ok);
                    let delete_files = futures::stream::iter(path_iter).boxed();
                    let results = s
                        .delete_stream(delete_files)
                        .try_collect::<Vec<_>>()
                        .await?;
                }

                maybe_batch
            })
            .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            input_stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
