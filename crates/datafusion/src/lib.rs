// Datafusion error will trigger this warning.
#![allow(clippy::result_large_err)]

mod commands;
pub mod config;
mod engine;
mod error;
mod execution;
mod planner;
mod schema_provider;
mod session;
pub mod sql;
pub mod table_provider;
mod unity;
mod utils;

// re-export the version type as it is part of public api of this crate.
pub use delta_kernel::Version;
pub use engine::DataFusionEngine;
pub use schema_provider::DeltaLakeSchemaProvider;
pub use session::{
    KernelContextExt, KernelExtensionConfig, KernelSessionExt, KernelTaskContextExt,
    ObjectStoreFactory,
};

#[cfg(test)]
pub(crate) mod tests {

    use std::path::PathBuf;
    use std::sync::Arc;

    use acceptance::read_dat_case;
    use datafusion::prelude::SessionContext;
    use delta_kernel::Engine;
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use rstest::*;

    use super::*;

    static SKIPPED_TESTS: &[&str; 1] = &["iceberg_compat_v1"];

    #[fixture]
    pub(crate) fn df_engine() -> (Arc<dyn Engine>, SessionContext) {
        let ctx = SessionContext::new();
        let engine = DataFusionEngine::<TokioBackgroundExecutor>::new_background(&ctx);
        (engine, ctx)
    }

    #[rstest]
    #[tokio::test]
    async fn read_dat(
        #[files("../../dat/out/reader_tests/generated/**/test_case_info.json")] path: PathBuf,
        df_engine: (Arc<dyn Engine>, SessionContext),
    ) {
        for skipped in SKIPPED_TESTS {
            if path.to_str().unwrap().contains(skipped) {
                println!("Skipping test: {}", skipped);
                return;
            }
        }

        let case = read_dat_case(path.parent().unwrap()).unwrap();
        let (engine, _) = df_engine;
        case.assert_metadata(engine.clone()).await.unwrap();
        acceptance::data::assert_scan_metadata(engine.clone(), &case)
            .await
            .unwrap();
    }
}
