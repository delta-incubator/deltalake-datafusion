use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::prelude::SessionConfig;

extensions_options! {
    /// Configuration for integration with Unity Catalog
    pub struct UnityCatalogConfig {
        /// Server Url for Unity Catalog
        pub uri: Option<String>, default = None
        /// Access Token (PAT) for Unity Catalog server
        pub token: Option<String>, default = None
    }
}

impl ConfigExtension for UnityCatalogConfig {
    const PREFIX: &'static str = "unity";
}

extensions_options! {
    pub struct RetentionDurationCheck {
        /// Denotes if the retention duration check is enabled.
        pub enabled: bool, default = true
    }
}

extensions_options! {
    pub struct DeltaLakeConfig {
        pub enable_caching: bool, default = false

        /// If enabled, prevents dangerous VACUUM operations from running on Delta tables.
        pub retention_duration_check: RetentionDurationCheck, default = RetentionDurationCheck::default()
    }
}

impl ConfigExtension for DeltaLakeConfig {
    const PREFIX: &'static str = "delta";
}

extensions_options! {
    pub struct OpenLakehouseConfig {
        /// Configuration to connect to unity catalog server
        pub unity: UnityCatalogConfig, default = UnityCatalogConfig::default()

        /// Configuration for interacting with delta tables
        pub delta: DeltaLakeConfig, default = DeltaLakeConfig::default()
    }
}

impl ConfigExtension for OpenLakehouseConfig {
    const PREFIX: &'static str = "lakehouse";
}

impl OpenLakehouseConfig {
    pub fn session_config() -> SessionConfig {
        SessionConfig::new().with_option_extension(OpenLakehouseConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::config::{ConfigOptions, Extensions};

    use super::*;

    #[test]
    fn test_delta_lake_config() {
        let mut extensions = Extensions::new();
        extensions.insert(OpenLakehouseConfig::default());

        let mut config = ConfigOptions::new().with_extensions(extensions);
        config
            .set("lakehouse.unity.uri", "http://example.com")
            .unwrap();
        config.set("lakehouse.unity.token", "token").unwrap();

        let lakehouse_config = config.extensions.get::<OpenLakehouseConfig>().unwrap();

        assert_eq!(
            lakehouse_config.unity.uri,
            Some("http://example.com".to_string())
        );
        assert_eq!(lakehouse_config.unity.token, Some("token".to_string()));
    }
}
