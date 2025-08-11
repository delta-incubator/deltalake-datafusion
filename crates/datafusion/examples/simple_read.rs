use std::sync::Arc;

use datafusion::arrow::util::pretty::print_batches;
use datafusion::catalog::MemoryCatalogProvider;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::CatalogProvider;
use deltalake_datafusion::{DeltaLakeSchemaProvider, KernelContextExt as _};
use url::Url;

static PATH: &str = "/Users/robert.pack/code/deltalake-datafusion/dat/out/reader_tests/generated/";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = Url::from_directory_path(std::fs::canonicalize(PATH)?).unwrap();
    let table_url = base.join("column_mapping/delta")?;

    let ctx = SessionContext::new().enable_delta_kernel(None);

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

    Ok(())
}
