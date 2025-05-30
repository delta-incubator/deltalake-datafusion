use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::arrow::util::pretty::print_batches;
use delta_kernel::Table;
use deltalake_datafusion::{DeltaLogTableProvider, KernelContextExt as _};
use url::Url;

static PATH: &str = "/Users/robert.pack/code/deltalake-datafusion/crates/datafusion/dat/out/reader_tests/generated/";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = Url::from_directory_path(std::fs::canonicalize(PATH)?).unwrap();
    let table_url = base.join("column_mapping/delta")?;

    let ctx = SessionContext::new().enable_delta_kernel(None);
    ctx.register_delta("delta_table", &table_url).await?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    let table = Table::try_from_uri(table_url)?;
    let log_provider = DeltaLogTableProvider::new(table.into())?;

    ctx.register_table("delta_log", Arc::new(log_provider))?;
    let df = ctx
        .sql(
            r#"
            WITH delta_log_pruned AS (
                SELECT 1 as key, "metaData", protocol
                FROM delta_log
                WHERE "protocol" IS NOT NULL OR "metaData" IS NOT NULL
            )
            SELECT "metaData", protocol
            FROM (
                SELECT "metaData", protocol
                FROM delta_log_pruned
                WHERE "metaData" IS NOT NULL
                LIMIT 1
            )
            UNION ALL
            SELECT "metaData", protocol
            FROM (
                SELECT "metaData", protocol
                FROM delta_log_pruned
                WHERE "protocol" IS NOT NULL
                LIMIT 1
            )
            "#,
        )
        .await?;
    let df = df.collect().await?;

    print_batches(&df)?;

    Ok(())
}
