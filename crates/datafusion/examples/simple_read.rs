use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::context::SessionContext;
use deltalake_datafusion::KernelContextExt as _;
use url::Url;

static PATH: &str = "/Users/robert.pack/code/deltalake-datafusion/crates/datafusion/dat/out/reader_tests/generated/";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = Url::from_directory_path(std::fs::canonicalize(PATH)?).unwrap();
    let table_url = base.join("with_checkpoint/delta")?;

    let ctx = SessionContext::new().enable_delta_kernel(None);
    ctx.register_delta("delta_table", &table_url).await?;

    let df = ctx.sql("SELECT * FROM delta_table").await?;
    let df = df.collect().await?;
    print_batches(&df)?;

    Ok(())
}
