// https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum#vacuum-a-delta-table
// https://docs.databricks.com/aws/en/delta/vacuum
// https://docs.databricks.com/aws/en/delta/history#data-retention

use std::sync::Arc;

use chrono::{Duration, Utc};
use datafusion::common::{Result, plan_datafusion_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::prelude::{col, lit, starts_with, substr};
use datafusion::scalar::ScalarValue;
use url::Url;

use crate::KernelSessionExt;
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

#[derive(Debug, thiserror::Error)]
enum VacuumError {
    #[error("Invalid inventory schema: {0}")]
    InvalidInventorySchema(String),
}

/// Plan the execution of a VACUUM command
///
/// VACUUM removes all files from the table directory that are not managed by Delta, as well as
/// data files that are no longer in the latest state of the transaction log for the table and are
/// older than a retention threshold. VACUUM will skip all directories that begin with an
/// underscore (`_`), which includes the `_delta_log`. Partitioning your table on a column that begins
/// with an underscore is an exception to this rule; VACUUM scans all valid partitions included in
/// the target Delta table. Delta table data files are deleted according to the time they have been
/// logically removed from Delta's transaction log plus retention hours, not their modification
/// timestamps on the storage system. The default threshold is 7 days.
pub(crate) async fn plan_vacuum(
    session: &SessionState,
    node: &VacuumPlanNode,
) -> Result<Arc<dyn ExecutionPlan>> {
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

    let snapshot = session
        .kernel_ext()?
        .read_snapshot_delta(&table_dir, None)
        .await?;

    let lh_config = session
        .kernel_ext()?
        .config()
        .ok_or(plan_datafusion_err!("open lakehouse config not found."))?;

    let retention_duration = match (
        snapshot.table_properties().deleted_file_retention_duration,
        node.retention_hours,
    ) {
        (Some(retention_duration), maybe_retention_hours) => {
            let retention_duration = Duration::new(
                retention_duration.as_secs() as i64,
                retention_duration.subsec_nanos(),
            )
            .ok_or(plan_datafusion_err!(
                "failed to convert retention duration property."
            ))?;
            match maybe_retention_hours {
                Some(retention_hours) => {
                    let duration = retention_hours_to_duration(retention_hours);
                    if lh_config.delta.retention_duration_check.enabled
                        && duration < retention_duration
                    {
                        return plan_err!(
                            "retention duration is shorter than the minimum required duration"
                        );
                    };
                    duration
                }
                None => retention_duration,
            }
        }
        (None, Some(retention_hours)) => retention_hours_to_duration(retention_hours),
        (None, None) => Duration::hours(7 * 24),
    };
    let min_ts = Utc::now() - retention_duration;

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

fn retention_hours_to_duration(retention_hours: f64) -> Duration {
    let hours = retention_hours.floor() as i64;
    let minutes = ((retention_hours - hours as f64) * 60.0).floor() as i64;
    Duration::hours(hours) + Duration::minutes(minutes)
}
