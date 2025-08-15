use std::{
    fmt,
    sync::{Arc, LazyLock},
};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    common::{DFSchema, DFSchemaRef, DataFusionError},
    error::Result,
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    prelude::Expr,
};

pub use catalogs::*;
use serde::Serialize;

use crate::exec::ExecutableUnityCatalogStement;

mod catalogs;

pub(crate) static CREATE_UC_RETURN_SCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    let arrow_schema = Schema::new(vec![
        Field::new("securable_name", DataType::Utf8, false),
        Field::new("securable_type", DataType::Utf8, false),
        Field::new("securable_object", DataType::Utf8, false),
    ]);
    DFSchemaRef::new(DFSchema::try_from(arrow_schema).unwrap())
});

pub(crate) static DROP_UC_RETURN_SCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    let arrow_schema = Schema::new(vec![
        Field::new("securable_name", DataType::Utf8, false),
        Field::new("securable_type", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
    ]);
    DFSchemaRef::new(DFSchema::try_from(arrow_schema).unwrap())
});

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum UnityCatalogStatement {
    CreateCatalog(CreateCatalogStatement),
    DropCatalog(DropCatalogStatement),
}

impl From<CreateCatalogStatement> for UnityCatalogStatement {
    fn from(value: CreateCatalogStatement) -> Self {
        UnityCatalogStatement::CreateCatalog(value)
    }
}

impl From<DropCatalogStatement> for UnityCatalogStatement {
    fn from(value: DropCatalogStatement) -> Self {
        UnityCatalogStatement::DropCatalog(value)
    }
}

impl UnityCatalogStatement {
    pub fn command_name(&self) -> &str {
        use UnityCatalogStatement::*;

        match &self {
            CreateCatalog(_) => "CreateCatalog",
            DropCatalog(_) => "DropCatalog",
        }
    }

    pub fn fmt_for_explain_params(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use UnityCatalogStatement::*;

        match self {
            CreateCatalog(cmd) => write!(f, "CreateCatalog: name={}", cmd.name),
            DropCatalog(cmd) => write!(
                f,
                "DropCatalog: name={} if_exists={} cascade={}",
                cmd.name,
                cmd.if_exists,
                cmd.cascade
            ),
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Debug, Clone)]
pub struct ExecuteUnityCatalogPlanNode {
    pub statement: UnityCatalogStatement,
}

impl UserDefinedLogicalNodeCore for ExecuteUnityCatalogPlanNode {
    fn name(&self) -> &str {
        self.statement.command_name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.statement.return_schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.statement.fmt_for_explain_params(f)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            Err(DataFusionError::Internal(
                "CreateCatalogPlanNode does not support exprs and inputs".to_string(),
            ))
        } else {
            Ok(self.clone())
        }
    }
}

pub(crate) fn create_response_to_batch(
    name: impl ToString,
    type_name: impl ToString,
    object: impl Serialize,
) -> Result<RecordBatch> {
    let names = vec![name.to_string()];
    let types = vec![type_name.to_string()];
    let values = vec![serde_json::to_string(&object).unwrap()];
    let schema = Arc::new(CREATE_UC_RETURN_SCHEMA.as_arrow().clone());
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(values)),
        ],
    )?)
}

pub(crate) fn drop_response_to_batch(
    name: impl ToString,
    type_name: impl ToString,
    status: impl ToString,
) -> Result<RecordBatch> {
    let names = vec![name.to_string()];
    let types = vec![type_name.to_string()];
    let status = vec![status.to_string()];
    let schema = Arc::new(DROP_UC_RETURN_SCHEMA.as_arrow().clone());
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(status)),
        ],
    )?)
}
