use std::{any::Any, sync::Arc};

use dashmap::DashMap;
use datafusion::execution::SessionState;
use datafusion_catalog::{SchemaProvider, TableProvider};
use datafusion_common::{DataFusionError, error::Result, exec_err};
use delta_kernel::Snapshot;
use parking_lot::RwLock;
use url::Url;

use crate::{DeltaTableProvider, KernelSessionExt as _};

#[derive(Debug)]
pub struct DeltaLakeSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
    state: Arc<RwLock<SessionState>>,
}

impl DeltaLakeSchemaProvider {
    pub fn new(state: Arc<RwLock<SessionState>>) -> Self {
        Self {
            tables: DashMap::new(),
            state,
        }
    }

    pub async fn register_delta(
        &self,
        name: impl ToString,
        url: &Url,
    ) -> Result<Arc<dyn TableProvider>> {
        self.state.read().ensure_object_store(url).await?;

        let mut url = url.clone();
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        let engine = self.state.read().kernel_engine()?;
        let snapshot = tokio::task::spawn_blocking(move || {
            Snapshot::try_new(url, engine.as_ref(), None)
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        })
        .await
        .map_err(|e| DataFusionError::Execution(e.to_string()))??;

        let provider = Arc::new(DeltaTableProvider::try_new(snapshot.into())?) as _;
        self.tables.insert(name.to_string(), Arc::clone(&provider));
        Ok(provider)
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DeltaLakeSchemaProvider {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.tables.iter().map(|e| e.key().clone()).collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(table) = self.tables.get(name) else {
            return Ok(None);
        };

        let (table, updated) = if let Some(provider) = table
            .value()
            .as_ref()
            .as_any()
            .downcast_ref::<DeltaTableProvider>()
        {
            let existing_snapshot = provider.current_snapshot().clone();
            let current_version = existing_snapshot.version();

            let engine = self.state.read().kernel_engine()?;
            let snapshot = tokio::task::spawn_blocking(move || {
                Snapshot::try_new_from(existing_snapshot, engine.as_ref(), None)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))
            })
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))??;

            if snapshot.version() > current_version {
                (Arc::new(DeltaTableProvider::try_new(snapshot)?) as _, true)
            } else {
                (Arc::clone(table.value()), false)
            }
        } else {
            (Arc::clone(table.value()), false)
        };

        if updated {
            self.tables.insert(name.to_string(), Arc::clone(&table));
        }
        Ok(Some(table))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return exec_err!("The table {name} already exists");
        }
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
