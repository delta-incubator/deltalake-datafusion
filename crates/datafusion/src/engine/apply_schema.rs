use std::sync::Arc;

use datafusion::arrow::compute::cast;
use itertools::Itertools;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, LargeListArray, LargeListViewArray, ListArray, ListViewArray,
    MapArray, RecordBatch, StructArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::datatypes::{Fields, Schema};
use datafusion::arrow::error::{ArrowError, Result};

#[cfg(test)]
#[path = "apply_schema_test.rs"]
mod tests;

// Apply a schema to an array. The array _must_ be a `StructArray`. Returns a `RecordBatch where the
// names of fields, nullable, and metadata in the struct have been transformed to match those in
// schema specified by `schema`
pub(crate) fn apply_schema(array: &dyn Array, schema: &Schema) -> Result<RecordBatch> {
    let applied = apply_schema_to_struct(array, schema.fields())?;
    let (fields, columns, nulls) = applied.into_parts();
    if let Some(nulls) = nulls {
        if nulls.null_count() != 0 {
            return Err(ArrowError::SchemaError(
                "Top-level nulls in struct are not supported".to_string(),
            ));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

// A helper that is a wrapper over `transform_field_and_col`. This will take apart the passed struct
// and use that method to transform each column and then put the struct back together. Target fields for
// each column should be passed in `target_fields`. This will try to extract the column from the struct
// and apply the schema to it. If the column is not found, it will return a new null column of the
// correct type.
fn transform_struct(struct_array: &StructArray, target_fields: &Fields) -> Result<StructArray> {
    let (transformed_fields, transformed_cols): (Vec<Arc<Field>>, Vec<ArrayRef>) = target_fields
        .iter()
        .map(
            |field| match extract_column(struct_array, &[field.name().clone()]) {
                Ok(arr) => {
                    let transformed_col = apply_schema_to(&arr, field.data_type())?;
                    Ok::<_, ArrowError>((field.clone(), transformed_col))
                }
                Err(_) => Ok((
                    field.clone(),
                    new_null_array(field.data_type(), struct_array.len()),
                )),
            },
        )
        .process_results(|iter| iter.unzip())?;

    StructArray::try_new(
        transformed_fields.into(),
        transformed_cols,
        struct_array.nulls().cloned(),
    )
}

// Transform a struct array. The data is in `array`, and the target fields are in `kernel_fields`.
fn apply_schema_to_struct(array: &dyn Array, fields: &Fields) -> Result<StructArray> {
    let Some(sa) = array.as_struct_opt() else {
        return Err(ArrowError::SchemaError(
            "Arrow claimed to be a struct but isn't a StructArray".to_string(),
        ));
    };
    transform_struct(sa, fields)
}

// deconstruct the array, then rebuild the mapped version
fn apply_schema_to_list_elements(
    array: &dyn Array,
    target_inner_type: &Field,
) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::List(_) => {
            let arr = array.as_list::<i32>();
            let (field, offset_buffer, values, nulls) = arr.clone().into_parts();
            let transformed_values = apply_schema_to(&values, target_inner_type.data_type())?;
            let transformed_field = Field::new(
                field.name(),
                transformed_values.data_type().clone(),
                target_inner_type.is_nullable(),
            );
            Ok(Arc::new(ListArray::try_new(
                Arc::new(transformed_field),
                offset_buffer,
                transformed_values,
                nulls,
            )?))
        }
        DataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            let (field, offset_buffer, values, nulls) = arr.clone().into_parts();
            let transformed_values = apply_schema_to(&values, target_inner_type.data_type())?;
            let transformed_field = Field::new(
                field.name(),
                transformed_values.data_type().clone(),
                target_inner_type.is_nullable(),
            );
            Ok(Arc::new(LargeListArray::try_new(
                Arc::new(transformed_field),
                offset_buffer,
                transformed_values,
                nulls,
            )?))
        }
        DataType::ListView(_) => {
            let arr = array.as_list_view::<i32>();
            let (field, value_offsets, value_sizes, values, nulls) = arr.clone().into_parts();
            let transformed_values = apply_schema_to(&values, target_inner_type.data_type())?;
            let transformed_field = Field::new(
                field.name(),
                transformed_values.data_type().clone(),
                target_inner_type.is_nullable(),
            );
            Ok(Arc::new(ListViewArray::try_new(
                Arc::new(transformed_field),
                value_offsets,
                value_sizes,
                transformed_values,
                nulls,
            )?))
        }
        DataType::LargeListView(_) => {
            let arr = array.as_list_view::<i64>();
            let (field, value_offsets, value_sizes, values, nulls) = arr.clone().into_parts();
            let transformed_values = apply_schema_to(&values, target_inner_type.data_type())?;
            let transformed_field = Field::new(
                field.name(),
                transformed_values.data_type().clone(),
                target_inner_type.is_nullable(),
            );
            Ok(Arc::new(LargeListViewArray::try_new(
                Arc::new(transformed_field),
                value_offsets,
                value_sizes,
                transformed_values,
                nulls,
            )?))
        }
        _ => Err(ArrowError::SchemaError(
            "Arrow claimed to be a list but isn't a ListArray".to_string(),
        )),
    }
}

// deconstruct a map, and rebuild it with the specified target kernel type
fn apply_schema_to_map(array: &dyn Array, inner_map_fields: &Field) -> Result<MapArray> {
    let Some(ma) = array.as_map_opt() else {
        return Err(ArrowError::SchemaError(
            "Arrow claimed to be a map but isn't a MapArray".to_string(),
        ));
    };
    let (map_field, offset_buffer, map_struct_array, nulls, ordered) = ma.clone().into_parts();
    let DataType::Struct(target_fields) = inner_map_fields.data_type() else {
        return Err(ArrowError::SchemaError(
            "Expected inner map field to be a struct".to_string(),
        ));
    };

    let transformed_arr = transform_struct(&map_struct_array, target_fields)?;
    let transformed_field = Field::new(
        map_field.name().clone(),
        transformed_arr.data_type().clone(),
        map_field.is_nullable(),
    );
    MapArray::try_new(
        Arc::new(transformed_field),
        offset_buffer,
        transformed_arr,
        nulls,
        ordered,
    )
}

// apply `schema` to `array`. This handles renaming, and adjusting nullability and metadata. if the
// actual data types don't match, this will return an error
pub(crate) fn apply_schema_to(array: &ArrayRef, schema: &DataType) -> Result<ArrayRef> {
    use DataType::*;
    let array: ArrayRef = match schema {
        Struct(fields) => Arc::new(apply_schema_to_struct(array, fields)?),
        Map(kv_type, _) => Arc::new(apply_schema_to_map(array, kv_type)?),
        List(element_type)
        | LargeList(element_type)
        | ListView(element_type)
        | LargeListView(element_type) => {
            // We manually map the inner field to handle imputation / selection of the inner field
            let withmapped_inner = Arc::new(apply_schema_to_list_elements(array, element_type)?);
            // We use the arrow methods to handle conversions between physical list types
            cast(withmapped_inner.as_ref(), schema)?
        }
        _ => cast(array, schema)?,
    };
    Ok(array)
}

/// Given a RecordBatch or StructArray, recursively probe for a nested column path and return the
/// corresponding column, or Err if the path is invalid. For example, given the following schema:
/// ```text
/// root: {
///   a: int32,
///   b: struct {
///     c: int32,
///     d: struct {
///       e: int32,
///       f: int64,
///     },
///   },
/// }
/// ```
/// The path ("b", "d", "f") would retrieve the int64 column while ("a", "b") would produce an error.
fn extract_column(mut parent: &StructArray, col: &[String]) -> Result<ArrayRef> {
    let mut field_names = col.iter();
    let Some(mut field_name) = field_names.next() else {
        return Err(ArrowError::SchemaError("Empty column path".to_string()))?;
    };
    loop {
        let child = parent
            .column_by_name(field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("No such field: {field_name}")))?;
        field_name = match field_names.next() {
            Some(name) => name,
            None => return Ok(child.clone()),
        };
        parent = child
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ArrowError::SchemaError(format!("Not a struct: {field_name}")))?;
    }
}
