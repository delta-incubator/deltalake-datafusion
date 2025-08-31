use std::sync::Arc;

use chrono::{Duration, Utc};
use datafusion::common::{Result, plan_datafusion_err};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::prelude::{col, lit, starts_with, substr};
use datafusion::scalar::ScalarValue;
use url::Url;

use crate::execution::{DIRECTORY_LISTING_RETURN_SCHEMA_DF, DirectoryListingExec};

pub use self::logical::{VacuumMode, VacuumStatement};
pub use self::physical::VacuumExec;

mod logical;
mod physical;

/// Alias for VacuumStatement
///
/// We define and alias because:
/// - more idiomatic naming in planner
/// - allows us to replace with a dedicated struct more easily.
pub type VacuumPlanNode = VacuumStatement;

pub(crate) async fn plan_vacuum(
    session: &SessionState,
    node: &VacuumPlanNode,
) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: get and validate default retention hours from snapshot.
    let retention_hours = node.retention_hours.unwrap_or(7.0 * 24.0);
    let hours = retention_hours.floor() as i64;
    let minutes = ((retention_hours - hours as f64) * 60.0).floor() as i64;
    let min_ts = Utc::now() - Duration::hours(hours) - Duration::minutes(minutes);

    let mut table_dir = if node.name.0.len() == 1 {
        let raw_url = &node.name.0[0]
            .as_ident()
            .ok_or(plan_datafusion_err!("Expected identifier"))?
            .value;
        Url::parse(raw_url)
            .map_err(|_| plan_datafusion_err!("failed to parse object name as url"))?
    } else {
        todo!("Implement multi-part table name support")
    };

    if !table_dir.path().ends_with('/') {
        table_dir.set_path(&format!("{}/", table_dir.path()));
    }

    let is_in_hidden_dir = starts_with(
        substr(
            col("path"),
            lit(ScalarValue::Int16(Some(table_dir.as_str().len() as i16))),
        ),
        lit(ScalarValue::Utf8(Some("_".to_string()))),
    );
    let expr = col("is_dir").is_false().and(
        col("modification_time")
            .lt(lit(ScalarValue::TimestampMillisecond(
                Some(min_ts.timestamp_millis()),
                Some("UTC".into()),
            )))
            .and(is_in_hidden_dir.is_false()),
    );
    let predicate = session.create_physical_expr(expr, &DIRECTORY_LISTING_RETURN_SCHEMA_DF)?;

    let files_plan: Arc<dyn ExecutionPlan> = match node.mode.as_ref().unwrap_or(&VacuumMode::Full) {
        VacuumMode::Full => Arc::new(DirectoryListingExec::new(table_dir.clone())),
        VacuumMode::Lite => todo!("Implement lite vacuum"),
    };
    let filtered_files = Arc::new(FilterExec::try_new(predicate, files_plan)?);

    if node.dry_run.unwrap_or(false) {
        return Ok(Arc::new(GlobalLimitExec::new(filtered_files, 0, Some(1000))) as _);
    }

    let store_url = &table_dir[..url::Position::BeforePath];
    let store_url = ObjectStoreUrl::parse(store_url)?;
    Ok(Arc::new(VacuumExec::new(store_url, filtered_files)) as _)
}
