use std::{
    any::Any,
    fmt,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use arrow::{
    array::{BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    common::{DFSchema, DFSchemaRef, Result, Statistics, internal_err},
    execution::{
        RecordBatchStream, SendableRecordBatchStream, TaskContext, object_store::ObjectStoreUrl,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
};
use futures::Stream;
use object_store::{ListResult, ObjectStore, path::Path};
use pin_project_lite::pin_project;
use url::Url;

static DIRECTORY_LISTING_RETURN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let arrow_schema = Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("length", DataType::UInt64, true),
        Field::new("is_dir", DataType::Boolean, false),
        Field::new(
            "modification_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]);
    arrow_schema.into()
});

pub(crate) static DIRECTORY_LISTING_RETURN_SCHEMA_DF: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    DFSchemaRef::new(DFSchema::try_from(DIRECTORY_LISTING_RETURN_SCHEMA.clone()).unwrap())
});

#[derive(Debug)]
pub struct DirectoryListingExec {
    directory: Url,
    cache: PlanProperties,
}

impl DirectoryListingExec {
    pub fn new(directory: Url) -> Self {
        Self {
            cache: PlanProperties::new(
                EquivalenceProperties::new(DIRECTORY_LISTING_RETURN_SCHEMA.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            directory,
        }
    }
}

impl DisplayAs for DirectoryListingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DirectoryListingExec: directory={}", self.directory)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DirectoryListingExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return internal_err!("DirectoryListingExec invalid partition {partition}");
        }

        let store_url = &self.directory[..url::Position::BeforePath];
        let store_url = ObjectStoreUrl::parse(store_url)?;
        let store = context.runtime_env().object_store(store_url)?;
        let prefix = Path::from_url_path(self.directory.path())?;

        Ok(Box::pin(DirectoryListingStream::new(
            self.directory.clone(),
            // NB: if we find that we need to be able to process in chunks,
            // we can move to using `PaginatedListStore` from the object_store crate.
            Box::pin(futures::stream::once(async move {
                store.list_with_delimiter(Some(&prefix)).await
            })),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

pin_project! {
    /// Converts a [`Stream`] of [`ObjectMeta`] into a [`SendableRecordBatchStream`]
    pub struct DirectoryListingStream<S> {
        directory: Url,

        #[pin]
        stream: S,
    }
}

impl<S> DirectoryListingStream<S> {
    pub fn new(mut directory: Url, stream: S) -> Self {
        if !directory.path().ends_with('/') {
            directory.set_path(&format!("{}/", directory.path()));
        };
        Self { directory, stream }
    }
}

impl<S> std::fmt::Debug for DirectoryListingStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryListingStream").finish()
    }
}

impl<S> Stream for DirectoryListingStream<S>
where
    S: Stream<Item = Result<ListResult, object_store::Error>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let directory = self.directory.clone();

        match self.project().stream.poll_next(cx) {
            Poll::Ready(Some(Ok(result))) => {
                let mut paths: Vec<String> = Vec::new();
                let mut lengths: Vec<Option<u64>> = Vec::new();
                let mut is_dirs: Vec<bool> = Vec::new();
                let mut times: Vec<Option<i64>> = Vec::new();

                for dir in result.common_prefixes {
                    // Safety: The directory path is guaranteed to be valid UTF-8.
                    let path = directory.join(dir.as_ref()).unwrap();
                    paths.push(path.to_string());
                    lengths.push(None);
                    times.push(None);
                    is_dirs.push(true);
                }

                for file in result.objects {
                    // Safety: The file path is guaranteed to be valid UTF-8.
                    let path = directory.join(file.location.as_ref()).unwrap();
                    paths.push(path.to_string());
                    lengths.push(Some(file.size));
                    times.push(Some(file.last_modified.timestamp_millis()));
                    is_dirs.push(false);
                }

                let batch = RecordBatch::try_new(
                    DIRECTORY_LISTING_RETURN_SCHEMA.clone(),
                    vec![
                        Arc::new(StringArray::from(paths)),
                        Arc::new(UInt64Array::from(lengths)),
                        Arc::new(BooleanArray::from(is_dirs)),
                        Arc::new(TimestampMillisecondArray::from(times)),
                    ],
                )?;

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<S> RecordBatchStream for DirectoryListingStream<S>
where
    S: Stream<Item = Result<ListResult, object_store::Error>>,
{
    fn schema(&self) -> SchemaRef {
        DIRECTORY_LISTING_RETURN_SCHEMA.clone()
    }
}
