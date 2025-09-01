use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err, not_impl_err, plan_datafusion_err, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    async_udf::AsyncScalarUDFImpl,
};
use datafusion_macros::user_doc;
use datafusion_session::SessionStore;
use http::Method;
use itertools::Itertools;
use object_store::ObjectStore;
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::signer::Signer;
use url::Url;

/// SignStorageUrl is a scalar user-defined function that signs a URL.
///
/// The resulting URL can be used to access the signed resource (file, directory/key).
#[user_doc(
    doc_section(label = "String Functions"),
    description = "Sign a URL",
    syntax_example = "sign_storage_url('https://example.com')"
)]
#[derive(Debug)]
pub struct SignStorageUrl {
    signature: Signature,
    session_store: Arc<SessionStore>,
}

impl SignStorageUrl {
    pub fn new(session_store: Arc<SessionStore>) -> Self {
        Self {
            session_store,
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8],
                Volatility::Volatile,
            ),
        }
    }
}

/// Implement the ScalarUDFImpl trait for AddOne
impl ScalarUDFImpl for SignStorageUrl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sign_storage_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !matches!(
            args.first(),
            Some(&DataType::Utf8) | Some(&DataType::LargeUtf8) | Some(&DataType::Utf8View)
        ) {
            return plan_err!("sign_storage_url only accepts string-like arguments");
        }
        // safety: we just checked above that the argument is a Some(..) variant
        Ok(args.first().unwrap().clone())
    }

    // The actual implementation would add one to the argument
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("SignStorageUrl can only be called from async contexts")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for SignStorageUrl {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _options: &ConfigOptions,
    ) -> Result<ArrayRef> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

        // we parse the url and split it into the base URL for the storage bucket
        // and the path within the bucket. We track the indices of the original values
        // in order to send only valid values to the storage provider and later
        // recinstruct the proper response.
        let parse_str = |(idx, value): (usize, Option<&str>)| {
            value.and_then(|v| {
                url::Url::parse(v).ok().and_then(|url| {
                    Some((
                        ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).ok()?,
                        (idx, Path::from_url_path(url.path()).ok()?),
                    ))
                })
            })
        };
        let urls: Vec<_> = if let Some(vals) = args[0].as_string_opt::<i32>() {
            vals.iter().enumerate().flat_map(parse_str).collect()
        } else if let Some(vals) = args[0].as_string_opt::<i64>() {
            vals.iter().enumerate().flat_map(parse_str).collect()
        } else if let Some(vals) = args[0].as_string_view_opt() {
            vals.iter().enumerate().flat_map(parse_str).collect()
        } else {
            return plan_err!("sign_storage_url only accepts string arguments");
        };

        let registry = self
            .session_store
            .get_session()
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("session store is not available"))?
            .read()
            .runtime_env()
            .object_store_registry
            .clone();
        // TODO: allow passing the desired duration as method argument
        let expires_in = Duration::new(60 * 60, 0);

        // we group all valid urls by their store and generate signed urls
        // for all urls under that store. The signers usually need to communicate
        // with the storage service only once to generate the signing key, after which
        // they can sign any number of urls without further communication.
        let store_map = urls.into_iter().into_group_map();
        let mut result_buffer = Vec::with_capacity(args[0].len());
        for (store_url, paths_and_idx) in store_map {
            let store = registry.get_store(store_url.as_ref())?;
            let (indices, paths): (Vec<_>, Vec<_>) = paths_and_idx.into_iter().unzip();
            let signed_urls =
                signed_urls(&store_url, store, &paths, Method::GET, expires_in).await?;
            result_buffer.extend(indices.into_iter().zip(signed_urls));
        }

        // construct a result vector from the individual store results.
        let mut results = vec![None; args[0].len()];
        for (i, url) in result_buffer.into_iter() {
            results[i] = Some(url.to_string());
        }

        // return the results as the same data type as the input array
        match args[0].data_type() {
            DataType::Utf8 => Ok(Arc::new(StringArray::from(results))),
            DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(results))),
            DataType::Utf8View => Ok(Arc::new(StringViewArray::from(results))),
            // safety: We limited the data types when we are reading the paths from the array.
            _ => unreachable!(),
        }
    }
}

// auxiliary trait to cast `ObjectStore` to `Any`.
trait DowncastableStore: ObjectStore + Any {
    fn as_any(&self) -> &dyn Any;
}

impl<T: ObjectStore + Any> DowncastableStore for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

async fn signed_urls(
    store_url: &ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
    paths: &[Path],
    method: Method,
    expires_in: Duration,
) -> Result<Vec<Url>> {
    if let Some(signer) = store.as_any().downcast_ref::<MicrosoftAzure>() {
        return Ok(signer.signed_urls(method, paths, expires_in).await?);
    }

    if let Some(signer) = store.as_any().downcast_ref::<AmazonS3>() {
        return Ok(signer.signed_urls(method, paths, expires_in).await?);
    }

    if let Some(signer) = store.as_any().downcast_ref::<GoogleCloudStorage>() {
        return Ok(signer.signed_urls(method, paths, expires_in).await?);
    }

    if store.as_any().downcast_ref::<LocalFileSystem>().is_some() {
        return Ok(paths
            .iter()
            .map(|path| AsRef::<Url>::as_ref(store_url).join(path.as_ref()).unwrap())
            .collect());
    }

    if store.as_any().downcast_ref::<InMemory>().is_some() {
        return Ok(paths
            .iter()
            .map(|path| AsRef::<Url>::as_ref(store_url).join(path.as_ref()).unwrap())
            .collect());
    }

    exec_err!("not a signing store")
}
