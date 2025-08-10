use std::sync::{Arc, Weak};

use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result as DFResult, TableReference};
use datafusion_execution::TaskContext;
use datafusion_execution::object_store::ObjectStoreRegistry;
use datafusion_session::{Session, SessionStore};
use delta_kernel::engine::default::executor::tokio::{
    TokioBackgroundExecutor, TokioMultiThreadExecutor,
};
use delta_kernel::object_store::ObjectStore;
use delta_kernel::{Engine, Snapshot, Version};
use parking_lot::RwLock;
use tokio::runtime::{Handle, RuntimeFlavor};
use url::Url;

use crate::engine::DataFusionEngine;
use crate::table_provider::{DeltaTableProvider, DeltaTableSnapshot, TableSnapshot};
use crate::utils::AsObjectStoreUrl;

/// Configuration for the kernel extension.
#[derive(Default)]
pub struct KernelExtensionConfig {
    context: Option<SessionContext>,
    /// The engine to use for the kernel.
    engine: Option<Arc<dyn Engine>>,
    /// The object store factory to use for the kernel.
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
    /// Runtime handle to execute blocking tasks.
    handle: Option<Handle>,
}

impl KernelExtensionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_context(mut self, context: SessionContext) -> Self {
        self.context = Some(context);
        self
    }

    pub fn with_engine(mut self, engine: impl Into<Option<Arc<dyn Engine>>>) -> Self {
        self.engine = engine.into();
        self
    }

    pub fn with_object_store_factory(
        mut self,
        factory: impl Into<Option<Arc<dyn ObjectStoreFactory>>>,
    ) -> Self {
        self.object_store_factory = factory.into();
        self
    }

    pub fn with_handle_multi_thread(mut self, handle: impl Into<Option<Handle>>) -> Self {
        self.handle = handle.into();
        self
    }

    pub fn build(self) -> SessionContext {
        let ctx = self.context.unwrap_or_default();
        let session_store = Arc::new(SessionStore::new());

        let engine = self.engine.unwrap_or_else(|| {
            let handle = self
                .handle
                .unwrap_or_else(|| tokio::runtime::Handle::current());
            match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => Arc::new(DataFusionEngine::new_with_session_store(
                    Arc::new(TokioMultiThreadExecutor::new(handle)),
                    session_store.clone(),
                )),
                RuntimeFlavor::CurrentThread => Arc::new(DataFusionEngine::new_with_session_store(
                    Arc::new(TokioBackgroundExecutor::new()),
                    session_store.clone(),
                )),
                _ => panic!("unsupported runtime flavor"),
            }
        });

        let ctx = with_engine(
            ctx,
            engine,
            session_store.clone(),
            self.object_store_factory,
        );
        session_store.with_state(ctx.state_weak_ref());

        ctx
    }
}

impl From<KernelExtensionConfig> for SessionContext {
    fn from(val: KernelExtensionConfig) -> Self {
        val.build()
    }
}

pub struct KernelExtension {
    pub(crate) engine: Arc<dyn Engine>,
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
    session_store: Arc<SessionStore>,
}

impl KernelExtension {
    /// Set the session state for the kernel extension.
    pub fn with_state(&self, state: Weak<RwLock<dyn Session>>) {
        self.session_store.with_state(state);
    }

    pub async fn read_snapshot(
        &self,
        url: &Url,
        version: Option<Version>,
    ) -> DFResult<Arc<dyn TableSnapshot>> {
        let url = url.clone();
        let engine = self.engine.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || Snapshot::try_new(url, engine.as_ref(), version))
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Arc::new(DeltaTableSnapshot::try_new(snapshot.into())?))
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for KernelExtension {
    async fn create_object_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>> {
        self.object_store_factory
            .as_ref()
            .ok_or_else(|| DataFusionError::Execution("no object store factory registered".into()))?
            .create_object_store(url)
            .await
    }
}

#[async_trait::async_trait]
impl<S: Session + ?Sized> KernelSessionExt for S {
    fn kernel_ext(&self) -> DFResult<Arc<KernelExtension>> {
        self.config()
            .get_extension::<KernelExtension>()
            .ok_or_else(|| DataFusionError::Execution("no engine extension found".into()))
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()> {
        let registry = self.runtime_env().object_store_registry.clone();
        let ext = self.kernel_ext()?;
        ensure_object_store(url, registry, ext).await
    }
}

#[async_trait::async_trait]
impl KernelTaskContextExt for TaskContext {
    fn kernel_ext(&self) -> DFResult<Arc<KernelExtension>> {
        self.session_config()
            .get_extension::<KernelExtension>()
            .ok_or_else(|| DataFusionError::Execution("no engine extension found".into()))
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()> {
        let registry = self.runtime_env().object_store_registry.clone();
        ensure_object_store(url, registry, self.kernel_ext()?).await
    }
}

#[async_trait::async_trait]
pub trait ObjectStoreFactory: Send + Sync + 'static {
    async fn create_object_store(&self, url: &Url) -> DFResult<Arc<dyn ObjectStore>>;
}

#[async_trait::async_trait]
pub trait KernelContextExt: private::KernelContextExtInner {
    fn enable_delta_kernel(
        self,
        config: impl Into<Option<KernelExtensionConfig>>,
    ) -> SessionContext;

    async fn read_delta_snapshot(
        &self,
        url: &Url,
        version: Option<Version>,
    ) -> DFResult<Arc<dyn TableSnapshot>>;

    async fn register_delta(
        &self,
        table_ref: impl Into<TableReference> + Send,
        url: &Url,
    ) -> DFResult<()>;
}

#[async_trait::async_trait]
impl KernelContextExt for SessionContext {
    fn enable_delta_kernel(
        self,
        config: impl Into<Option<KernelExtensionConfig>>,
    ) -> SessionContext {
        if self.state_ref().read().kernel_ext().is_err() {
            config.into().unwrap_or_default().with_context(self).into()
        } else {
            self
        }
    }

    async fn read_delta_snapshot(
        &self,
        url: &Url,
        version: Option<Version>,
    ) -> DFResult<Arc<dyn TableSnapshot>> {
        self.ensure_object_store(url).await?;
        let ext = self.kernel().read().kernel_ext()?;
        ext.read_snapshot(url, version).await
    }

    async fn register_delta(
        &self,
        table_ref: impl Into<TableReference> + Send,
        url: &Url,
    ) -> DFResult<()> {
        self.ensure_object_store(url).await?;

        let mut url = url.clone();
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        let engine = self.kernel_engine()?;

        // NB: Engine needs to list all fields and read some log,
        // so we need to run it in a blocking thread.
        let snapshot =
            tokio::task::spawn_blocking(move || Snapshot::try_new(url, engine.as_ref(), None))
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let provider = DeltaTableProvider::try_new(snapshot.into())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.register_table(table_ref, Arc::new(provider))?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait KernelSessionExt: Send + Sync {
    /// Get the engine extension for this session.
    ///
    /// Tries to get the kernel extension from the extension registry
    /// on [`SessionConfig`].
    fn kernel_ext(&self) -> DFResult<Arc<KernelExtension>>;

    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        Ok(self.kernel_ext()?.engine.clone())
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()>;
}

// NB This is the same as KernelSessionExt but for TaskContext.
// Since we need to implement KernelSessionExt for the `Session` trait,
// we need to implement a new trait for `TaskContext` as the compiler
// will complain, that TaskContext may eventually implement Session.
#[async_trait::async_trait]
pub trait KernelTaskContextExt: Send + Sync {
    /// Get the engine extension for this session.
    ///
    /// Tries to get the kernel extension from the extension registry
    /// on [`SessionConfig`].
    fn kernel_ext(&self) -> DFResult<Arc<KernelExtension>>;

    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        Ok(self.kernel_ext()?.engine.clone())
    }

    async fn ensure_object_store(&self, url: &Url) -> DFResult<()>;
}

use private::KernelContextExtInner as _;
mod private {
    use super::*;

    #[async_trait::async_trait]
    pub trait KernelContextExtInner {
        /// Ensure that an object store is available for the given url.
        ///
        /// If no object store is currently registered, attempts to discover
        /// suitable credentials to construct an object store.
        async fn ensure_object_store(&self, url: &Url) -> DFResult<()>;

        fn kernel(&self) -> Arc<RwLock<dyn KernelSessionExt>>;

        fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>>;
    }
}

#[async_trait::async_trait]
impl private::KernelContextExtInner for SessionContext {
    async fn ensure_object_store(&self, url: &Url) -> DFResult<()> {
        let ext = self.kernel().read().kernel_ext()?;
        let registry = self.runtime_env().object_store_registry.clone();
        ensure_object_store(url, registry, ext).await
    }

    fn kernel(&self) -> Arc<RwLock<dyn KernelSessionExt>> {
        self.state_ref()
    }

    fn kernel_engine(&self) -> DFResult<Arc<dyn Engine>> {
        self.kernel().read().kernel_engine()
    }
}

async fn ensure_object_store(
    url: &Url,
    registry: Arc<dyn ObjectStoreRegistry>,
    kernel: Arc<KernelExtension>,
) -> DFResult<()> {
    let object_store_url = url.as_object_store_url();
    if registry.get_store(object_store_url.as_ref()).is_err() {
        tracing::debug!("creating new object store for '{}'", url);
        let store = kernel.create_object_store(url).await?;
        registry.register_store(object_store_url.as_ref(), store);
    }

    Ok(())
}

fn with_engine(
    ctx: SessionContext,
    engine: Arc<dyn Engine>,
    session_store: Arc<SessionStore>,
    object_store_factory: Option<Arc<dyn ObjectStoreFactory>>,
) -> SessionContext {
    let session_id = ctx.session_id().clone();
    let mut new_config = ctx.copied_config();
    new_config.set_extension(Arc::new(KernelExtension {
        engine,
        object_store_factory,
        session_store,
    }));
    let ctx: SessionContext = ctx
        .into_state_builder()
        .with_session_id(session_id)
        .with_config(new_config)
        .build()
        .into();
    ctx
}
