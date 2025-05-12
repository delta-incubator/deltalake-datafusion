mod engine;
mod error;
mod expressions;
mod utils;

// re-export the version type as it is part of public api of this crate.
pub use delta_kernel::Version;
pub use engine::DataFusionEngine;

#[cfg(test)]
#[cfg(test)]
pub(crate) mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use acceptance::read_dat_case_info;
    use datafusion::prelude::SessionContext;
    use delta_kernel::Engine;
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use rstest::*;

    use super::*;

    static SKIPPED_TESTS: &[&str; 5] = &[
        "iceberg_compat_v1",
        "no_stats",
        "no_replay",
        "stats_as_struct",
        "with_checkpoint",
    ];

    #[fixture]
    pub(crate) fn df_engine() -> (Arc<dyn Engine>, SessionContext) {
        let ctx = SessionContext::new();
        let engine = DataFusionEngine::<TokioBackgroundExecutor>::new_background(&ctx);
        (engine, ctx)
    }

    #[rstest]
    #[tokio::test]
    async fn read_dat_case(
        #[files("dat/out/reader_tests/generated/**/test_case_info.json")] path: PathBuf,
        df_engine: (Arc<dyn Engine>, SessionContext),
    ) {
        for skipped in SKIPPED_TESTS {
            if path.to_str().unwrap().contains(skipped) {
                println!("Skipping test: {}", skipped);
                return;
            }
        }
        let case = read_dat_case_info(path).unwrap();
        let (engine, _) = df_engine;
        case.assert_metadata(engine.clone()).await.unwrap();
        acceptance::data::assert_scan_metadata(engine.clone(), &case)
            .await
            .unwrap();
    }
}
