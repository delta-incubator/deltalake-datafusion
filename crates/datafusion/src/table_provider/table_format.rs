use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::error::Result;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use delta_kernel::scan::state::Stats;
use delta_kernel::schema::SchemaRef as DeltaSchemaRef;
use delta_kernel::{ExpressionRef, Predicate, PredicateRef, Version};
use url::Url;

/// Metadata to read a data file from object storage.
pub struct ScanFileContext {
    /// Fully qualified URL of the file.
    pub file_url: Url,
    /// Size of the file on disk.
    pub size: u64,
    /// Selection vector to filter the data in the file.
    pub selection_vector: Option<Vec<bool>>,
    /// Transformations to apply to the data in the file.
    pub transform: Option<ExpressionRef>,
    /// Statistics about the data in the file.
    ///
    /// The query engine may choose to use these statistics to further optimize the scan.
    pub stats: Option<Stats>,
}

/// The metadata required to plan a table scan.
pub struct TableScan {
    /// Files included in the scan.
    ///
    /// These are filtered on a best effort basis to match the predicate passed to the scan.
    /// Files are grouped by the object store they are stored in.
    pub files: Vec<ScanFileContext>,
    /// The physical schema of the table.
    ///
    /// Data read from disk should be presented in this schema.
    /// While in most cases this corresponds to the data present in the file,
    /// the reader may be required to cast data to include columns not present
    /// in the file (i.e. the table may have undergone schema evolution),
    /// or type widening may have been applied.
    pub physical_schema: ArrowSchemaRef,
    /// The physical predicate of the table.
    ///
    /// Data read from disk can be filtered by this predicate. It must be applied before
    /// applying the file level transformations that are part of the `ScanFileContext`.
    /// The physical predicate is a potentially transformed version of the prodicate used
    /// when generating the scan. The transformations include renaming columns to their
    /// physical names in the file and omitting virtual / generated columns not present in the file.
    /// It is most useful when pushed into the parquet read to avoid loading unnecessary data.
    pub physical_predicate: Option<PredicateRef>,
    /// The logical schema of the table.
    ///
    /// This is the schema as it is presented to the end user.
    /// This includes any generated or implicit columns, mapped names, etc.
    pub logical_schema: ArrowSchemaRef,
}

#[async_trait::async_trait]
pub trait TableSnapshot: std::fmt::Debug + Send + Sync {
    /// The logical schema of the table.
    ///
    /// This is the fully resolved schema as it is presented to the end user.
    /// This includes any generated or implicit columns, mapped names, etc.
    fn table_schema(&self) -> &ArrowSchemaRef;

    fn schema(&self) -> DeltaSchemaRef;

    fn version(&self) -> Version;

    fn metadata(&self) -> &Metadata;

    fn protocol(&self) -> &Protocol;

    /// Produce the metadata required to plan a table scan.
    async fn scan_metadata(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        predicate: Arc<Predicate>,
    ) -> Result<TableScan>;
}
