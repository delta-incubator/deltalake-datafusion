use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::Result;

use super::{apply_schema, extract_column};

#[test]
fn test_extract_column_simple() -> Result<()> {
    // Create a simple struct with two fields
    let a = Int32Array::from(vec![1, 2, 3]);
    let b = Int64Array::from(vec![4, 5, 6]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        None,
    )?;

    // Test extracting a top-level field
    let result = extract_column(&struct_array, &["a".to_string()])?;
    assert_eq!(result.data_type(), &DataType::Int32);

    // Test extracting another top-level field
    let result = extract_column(&struct_array, &["b".to_string()])?;
    assert_eq!(result.data_type(), &DataType::Int64);

    Ok(())
}

#[test]
fn test_extract_column_nested() -> Result<()> {
    // Create a nested struct
    let inner_a = Int32Array::from(vec![1, 2, 3]);
    let inner_b = Int64Array::from(vec![4, 5, 6]);

    let inner_struct = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(inner_a), Arc::new(inner_b)],
        None,
    )?;

    let outer_struct = StructArray::try_new(
        vec![Arc::new(Field::new(
            "inner",
            inner_struct.data_type().clone(),
            false,
        ))]
        .into(),
        vec![Arc::new(inner_struct)],
        None,
    )?;

    // Test extracting a nested field
    let result = extract_column(&outer_struct, &["inner".to_string(), "a".to_string()])?;
    assert_eq!(result.data_type(), &DataType::Int32);

    // Test extracting another nested field
    let result = extract_column(&outer_struct, &["inner".to_string(), "b".to_string()])?;
    assert_eq!(result.data_type(), &DataType::Int64);

    Ok(())
}

#[test]
fn test_extract_column_errors() -> Result<()> {
    // Create a simple struct
    let a = Int32Array::from(vec![1, 2, 3]);
    let b = Int64Array::from(vec![4, 5, 6]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        None,
    )?;

    // Test empty path
    assert!(extract_column(&struct_array, &[]).is_err());

    // Test non-existent field
    assert!(extract_column(&struct_array, &["c".to_string()]).is_err());

    // Test trying to access a non-struct field as if it were a struct
    assert!(extract_column(&struct_array, &["a".to_string(), "b".to_string()]).is_err());

    Ok(())
}

#[test]
fn test_extract_column_deeply_nested() -> Result<()> {
    // Create a deeply nested struct (3 levels)
    let leaf = Int32Array::from(vec![1, 2, 3]);

    let level3 = StructArray::try_new(
        vec![Arc::new(Field::new("leaf", DataType::Int32, false))].into(),
        vec![Arc::new(leaf)],
        None,
    )?;

    let level2 = StructArray::try_new(
        vec![Arc::new(Field::new(
            "level3",
            level3.data_type().clone(),
            false,
        ))]
        .into(),
        vec![Arc::new(level3)],
        None,
    )?;

    let level1 = StructArray::try_new(
        vec![Arc::new(Field::new(
            "level2",
            level2.data_type().clone(),
            false,
        ))]
        .into(),
        vec![Arc::new(level2)],
        None,
    )?;

    // Test extracting a deeply nested field
    let result = extract_column(
        &level1,
        &[
            "level2".to_string(),
            "level3".to_string(),
            "leaf".to_string(),
        ],
    )?;
    assert_eq!(result.data_type(), &DataType::Int32);

    Ok(())
}

#[test]
fn test_apply_schema_basic() -> Result<()> {
    // Create input struct array
    let a = Int32Array::from(vec![1, 2, 3]);
    let b = Int64Array::from(vec![4, 5, 6]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        None,
    )?;

    // Create target schema with same structure
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int64, false),
    ]);

    // Apply schema
    let batch = apply_schema(&struct_array, &schema)?;

    // Verify result
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.column(0).data_type(), &DataType::Int32);
    assert_eq!(batch.column(1).data_type(), &DataType::Int64);
    assert_eq!(batch.schema().field(0).name(), "a");
    assert_eq!(batch.schema().field(1).name(), "b");

    Ok(())
}

#[test]
fn test_apply_schema_nullable_fields() -> Result<()> {
    // Create input struct array with nullable fields
    let a = Int32Array::from(vec![Some(1), None, Some(3)]);
    let b = Int64Array::from(vec![Some(4), Some(5), None]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Int64, true)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        None,
    )?;

    // Create target schema with nullable fields
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int64, true),
    ]);

    // Apply schema
    let batch = apply_schema(&struct_array, &schema)?;

    // Verify result
    assert_eq!(batch.num_columns(), 2);
    assert!(batch.schema().field(0).is_nullable());
    assert!(batch.schema().field(1).is_nullable());
    assert_eq!(batch.column(0).null_count(), 1);
    assert_eq!(batch.column(1).null_count(), 1);

    Ok(())
}

#[test]
fn test_apply_schema_top_level_nulls() -> Result<()> {
    // Create input struct array with top-level nulls
    let a = Int32Array::from(vec![1, 2, 3]);
    let b = Int64Array::from(vec![4, 5, 6]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        Some(NullBuffer::from(vec![true, false, true])),
    )?;

    // Create target schema
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int64, false),
    ]);

    // Apply schema should fail due to top-level nulls
    assert!(apply_schema(&struct_array, &schema).is_err());

    Ok(())
}

#[test]
fn test_apply_schema_missing_fields() -> Result<()> {
    // Create input struct array with fewer fields
    let a = Int32Array::from(vec![1, 2, 3]);

    let struct_array = StructArray::try_new(
        vec![Arc::new(Field::new("a", DataType::Int32, false))].into(),
        vec![Arc::new(a)],
        None,
    )?;

    // Create target schema with more fields
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int64, true),
    ]);

    // Apply schema
    let batch = apply_schema(&struct_array, &schema)?;

    // Verify result - missing field should be filled with nulls
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.column(0).data_type(), &DataType::Int32);
    assert_eq!(batch.column(1).data_type(), &DataType::Int64);
    assert_eq!(batch.column(1).null_count(), 3); // All values should be null

    Ok(())
}

#[test]
fn test_apply_schema_type_mismatch() -> Result<()> {
    // Create input struct array
    let a = Int32Array::from(vec![1, 2, 3]);
    let b = Int64Array::from(vec![4, 5, 6]);

    let struct_array = StructArray::try_new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ]
        .into(),
        vec![Arc::new(a), Arc::new(b)],
        None,
    )?;

    // Create target schema with mismatched types
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new(
            "c",
            DataType::List(Arc::new(Field::new("inner", DataType::Int64, false))),
            false,
        ),
    ]);

    // Apply schema should fail due to type mismatch
    assert!(apply_schema(&struct_array, &schema).is_err());

    Ok(())
}
