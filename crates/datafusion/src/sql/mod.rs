mod commands;
mod parser;
mod unity;

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::logical_expr::{Extension, LogicalPlan};

pub use parser::*;
pub use unity::*;

pub fn uc_statement_to_plan(statement: UnityCatalogStatement) -> Result<LogicalPlan> {
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(ExecuteUnityCatalogPlanNode { statement }),
    }))
}
