use arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use sqlparser::ast::{ObjectName, Value};
use unitycatalog_client::UnityCatalogClient;
use url::Url;

use crate::sql::{create_response_to_batch, drop_response_to_batch};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Mode {
    Full,
    Lite,
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Full
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct VacuumStatement {
    pub name: ObjectName,
    pub mode: Option<Mode>,
    pub retention_hours: Option<f64>,
    pub dry_run: Option<bool>,
}
