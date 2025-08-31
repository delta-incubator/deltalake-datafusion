use std::hash::Hasher;
use std::{fmt, sync::LazyLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DFSchema;
use datafusion::{
    common::{DFSchemaRef, Result, internal_err},
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    prelude::Expr,
};
use ordered_float::OrderedFloat;
use sqlparser::ast::ObjectName;

/// Alias for VacuumStatement
///
/// We define and alias because:
/// - more idiomatic naming in planner
/// - allows us to replace with a dedicated struct more easily.
pub type VacuumPlanNode = VacuumStatement;

pub(crate) static VACUUM_RETURN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let arrow_schema = Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_value", DataType::Utf8, false),
    ]);
    arrow_schema.into()
});
pub(crate) static VACUUM_RETURN_SCHEMA_DF: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(VACUUM_RETURN_SCHEMA.clone()).unwrap()));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum VacuumMode {
    Full,
    Lite,
}

impl fmt::Display for VacuumMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VacuumMode::Full => write!(f, "FULL"),
            VacuumMode::Lite => write!(f, "LITE"),
        }
    }
}

impl Default for VacuumMode {
    fn default() -> Self {
        VacuumMode::Full
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct VacuumStatement {
    pub name: ObjectName,
    pub mode: Option<VacuumMode>,
    pub retention_hours: Option<f64>,
    pub dry_run: Option<bool>,
}

impl Eq for VacuumStatement {}

impl std::hash::Hash for VacuumStatement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            name,
            mode,
            retention_hours,
            dry_run,
        } = self;
        let tuple = (name, mode, retention_hours.map(OrderedFloat), dry_run);
        tuple.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for VacuumStatement {
    fn name(&self) -> &str {
        "Vacuum"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &VACUUM_RETURN_SCHEMA_DF
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Vacuum: table={} mode={} retention_hours={:?} dry_run={}",
            self.name,
            self.mode.as_ref().unwrap_or(&VacuumMode::Full),
            self.retention_hours,
            self.dry_run.unwrap_or(false)
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            internal_err!("VacuumStatement plan node does not support exprs and inputs")
        } else {
            Ok(self.clone())
        }
    }
}
