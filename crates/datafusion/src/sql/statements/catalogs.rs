use arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use sqlparser::ast::{ObjectName, Value};
use unitycatalog_common::client::UnityCatalogClient;
use url::Url;

use crate::sql::{create_response_to_batch, drop_response_to_batch};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CreateCatalogStatement {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub using_share: Option<ObjectName>,
    pub managed_location: Option<Url>,
    pub default_collation: Option<String>,
    pub comment: Option<String>,
    pub options: Option<Vec<(String, Value)>>,
}

impl CreateCatalogStatement {
    pub(crate) async fn execute(&self, client: UnityCatalogClient) -> Result<RecordBatch> {
        let name = self.name.to_string();
        let catalog_info = if let Some(location) = self.managed_location.as_ref() {
            // create a calog with explicit managed location
            client
                .create_catalog(&name, Some(location), self.comment.as_ref(), None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else if let Some(share) = self.using_share.as_ref() {
            // create a catalog based on a delta share
            if !share.0.len() == 2 {
                return Err(DataFusionError::Execution(
                    "Expected share name to have exactly two segments. <provider>.<share>"
                        .to_string(),
                ));
            }
            let provider_name = share.0[0].to_string();
            let share_name = share.0[1].to_string();
            client
                .create_sharing_catalog(
                    &name,
                    provider_name,
                    share_name,
                    self.comment.as_ref(),
                    None,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            // create a catalog with default settings
            client
                .create_catalog(&name, None::<String>, self.comment.as_ref(), None)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        };
        create_response_to_batch(name, "Catalog", catalog_info)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct DropCatalogStatement {
    pub name: ObjectName,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropCatalogStatement {
    pub(crate) async fn execute(&self, client: UnityCatalogClient) -> Result<RecordBatch> {
        let name = self.name.to_string();
        client
            .catalog(&name)
            .delete(self.cascade)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop_response_to_batch(name, "Catalog", "success")
    }
}
