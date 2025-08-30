use std::{any::Any, fmt, sync::Arc};

use chrono::{DateTime, Utc};
use datafusion::{
    common::{Result, Statistics},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
};

use crate::sql::VACUUM_RETURN_SCHEMA;

#[derive(Debug)]
pub struct VacuumExec {
    min_retention_timestamp: DateTime<Utc>,
    dry_run: bool,
    inventory_plan: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl VacuumExec {
    pub fn new(
        min_retention_timestamp: DateTime<Utc>,
        dry_run: bool,
        inventory_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            min_retention_timestamp,
            dry_run,
            inventory_plan,
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
                write!(f, "VacuumExec: inventory_plan={:?}", self.inventory_plan)
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
        todo!()
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
