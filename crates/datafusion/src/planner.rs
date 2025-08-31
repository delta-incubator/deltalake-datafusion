use std::sync::Arc;

use datafusion::{
    common::{not_impl_err, plan_datafusion_err, plan_err},
    error::Result,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use sqlparser::dialect::dialect_from_str;
use tracing::debug;

use crate::{
    KernelSessionExt,
    commands::{VacuumPlanNode, plan_vacuum},
    sql::{ExecuteUnityCatalogPlanNode, HFParserBuilder, Statement, uc_statement_to_plan},
    unity::UnityCatalogRequestExec,
};

#[derive(Debug)]
pub struct OpenLakehouseQueryPlanner {}

#[async_trait::async_trait]
impl QueryPlanner for OpenLakehouseQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            OpenLakehousePlanner {},
        )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

pub struct OpenLakehousePlanner {}

impl OpenLakehousePlanner {
    fn handle_uc(&self, uc_node: &ExecuteUnityCatalogPlanNode) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnityCatalogRequestExec::new(Arc::new(
            uc_node.statement.clone(),
        ))) as _)
    }
}

#[async_trait::async_trait]
impl ExtensionPlanner for OpenLakehousePlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(uc_node) = node.as_any().downcast_ref::<ExecuteUnityCatalogPlanNode>() {
            if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
                return plan_err!(
                    "ExecuteUnityCatalogPlanNode expects no logical or physical inputs"
                );
            }
            debug!("Planning unity request: {:?}", uc_node);
            return Ok(Some(self.handle_uc(uc_node)?));
        }

        if let Some(vacuum_node) = node.as_any().downcast_ref::<VacuumPlanNode>() {
            if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
                return plan_err!("VacuumPlanNode expects no logical or physical inputs");
            }
            debug!("Planning VACUUM: {:?}", vacuum_node);
            return Ok(Some(plan_vacuum(session_state, vacuum_node).await?));
        }

        Ok(None)
    }
}

#[async_trait::async_trait]
pub(crate) trait SessionStateExt {
    fn sql_to_statement_lh(&self, sql: &str, dialecrt: &str) -> Result<Statement>;
    async fn create_logical_plan_lh(&self, sql: &str) -> Result<LogicalPlan>;
}

#[async_trait::async_trait]
impl SessionStateExt for SessionState {
    fn sql_to_statement_lh(&self, sql: &str, dialect: &str) -> Result<Statement> {
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                     Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                     MsSQL, ClickHouse, BigQuery, Ansi, DuckDB, Databricks."
            )
        })?;

        let recursion_limit = self.config().options().sql_parser.recursion_limit;

        let mut statements = HFParserBuilder::new(sql)
            .with_dialect(dialect.as_ref())
            .with_recursion_limit(recursion_limit)
            .build()?
            .parse_statements()?;

        if statements.len() > 1 {
            return not_impl_err!("The context currently only supports a single SQL statement");
        }

        let statement = statements.pop_front().ok_or_else(|| {
            plan_datafusion_err!("No SQL statements were provided in the query string")
        })?;
        Ok(statement)
    }

    async fn create_logical_plan_lh(&self, sql: &str) -> Result<LogicalPlan> {
        let dialect = self.config().options().sql_parser.dialect.as_str();
        let statement = self.sql_to_statement_lh(sql, dialect)?;
        match statement {
            Statement::DFStatement(statement) => {
                self.statement_to_plan(statement.as_ref().clone()).await
            }
            Statement::UnityCatalog(statement) => uc_statement_to_plan(statement),
            Statement::Vacuum(statement) => Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(statement),
            })),
        }
    }
}
