use std::sync::Arc;

use datafusion::arrow::{
    array::{RecordBatch, StructArray},
    datatypes::{Schema, SchemaRef},
};
use datafusion::common::{ColumnStatistics, Result};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use delta_kernel::engine::arrow_data::fix_nested_null_masks;

use self::apply_schema::apply_schema;

mod apply_schema;

#[derive(Debug)]
pub struct NestedSchemaMapper {
    pub(crate) projected_table_schema: SchemaRef,
}

impl SchemaMapper for NestedSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let struct_arr: StructArray = batch.into();
        let transformed = apply_schema(&struct_arr, self.projected_table_schema.as_ref()).unwrap();
        Ok(fix_nested_null_masks(transformed.into()).into())
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct NestedSchemaAdapterFactory;

impl Default for NestedSchemaAdapterFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NestedSchemaAdapterFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl SchemaAdapterFactory for NestedSchemaAdapterFactory {
    /// Create a [`SchemaAdapter`]
    ///
    /// Arguments:
    ///
    /// * `projected_table_schema`: The schema for the table, projected to
    ///   include only the fields being output (projected) by the this mapping.
    ///
    /// * `table_schema`: The entire table schema for the table
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(NestedSchemaAdapter {
            projected_table_schema,
        })
    }
}

pub struct NestedSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output
    /// (projected) by the associated ParquetSource
    projected_table_schema: SchemaRef,
}

impl SchemaAdapter for NestedSchemaAdapter {
    fn map_column_index(&self, _index: usize, _file_schema: &Schema) -> Option<usize> {
        todo!("map_column_index: this method seems to be unused - unless this gets raised.")
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if self
                .projected_table_schema
                .fields()
                .find(file_field.name())
                .is_some()
            {
                projection.push(file_idx);
            }
        }
        let nested_schema_mapping = NestedSchemaMapper {
            projected_table_schema: Arc::clone(&self.projected_table_schema),
        };

        Ok((Arc::new(nested_schema_mapping), projection))
    }
}
