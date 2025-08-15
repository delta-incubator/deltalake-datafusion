use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::execution::context::SessionContext;
use deltalake_datafusion::config::OpenLakehouseConfig;
use deltalake_datafusion::{DeltaLakeSchemaProvider, KernelContextExt as _};
use url::Url;

static PATH: &str = "/Users/robert.pack/code/deltalake-datafusion/dat/out/reader_tests/generated/";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = Url::from_directory_path(std::fs::canonicalize(PATH)?).unwrap();
    let table_url = base.join("column_mapping/delta")?;

    let config = OpenLakehouseConfig::session_config().set_str(
        "lakehouse.unity.uri",
        "http://localhost:8080/api/2.1/unity-catalog/",
    );

    let ctx = SessionContext::new_with_config(config).enable_delta_kernel(None);

    // register and read a single table
    // TODO: right now the table provider does not internally refresh the snapshot
    // when a table scan is initiated.
    ctx.register_delta("delta_table", &table_url).await?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    let schema = DeltaLakeSchemaProvider::new(ctx.state_ref());
    schema.register_delta("delta_table", &table_url).await?;
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("delta", Arc::new(schema))?;
    ctx.register_catalog("delta", catalog);

    let df = ctx.sql("SELECT * FROM delta.delta.delta_table").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    create_catalog(&ctx).await?;

    Ok(())
}

async fn create_catalog(ctx: &SessionContext) -> Result<(), Box<dyn std::error::Error>> {
    let df = ctx.sql_delta("CREATE CATALOG hello_from_df").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    let df = ctx.sql_delta("DROP CATALOG hello_from_df").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    Ok(())
}
