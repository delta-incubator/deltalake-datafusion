use std::sync::Arc;

use datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Operator, utils::conjunction};
use delta_kernel::expressions::{
    BinaryExpression, BinaryOperator, DecimalData, Expression, JunctionExpression,
    JunctionOperator, Scalar, UnaryExpression, UnaryOperator,
};
use delta_kernel::schema::{DataType, DecimalType, PrimitiveType};

use crate::error::to_df_err;

pub(crate) fn to_delta_predicate(filters: &[Expr]) -> DFResult<Arc<Expression>> {
    let Some(expr) = conjunction(filters.iter().cloned()) else {
        return Ok(Arc::new(Expression::Literal(Scalar::Boolean(true))));
    };
    to_delta_expression(&expr).map(Arc::new)
}

/// Convert a DataFusion expression to a Delta expression.
pub(crate) fn to_delta_expression(expr: &Expr) -> DFResult<Expression> {
    match expr {
        Expr::Column(column) => Ok(Expression::Column(
            column
                .name
                .parse()
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        )),
        Expr::Literal(scalar) => Ok(Expression::Literal(datafusion_scalar_to_scalar(scalar)?)),
        Expr::BinaryExpr(BinaryExpr {
            op: op @ (Operator::And | Operator::Or),
            ..
        }) => {
            let exprs = flatten_junction_expr(expr, *op)?;
            Ok(Expression::Junction(JunctionExpression {
                op: to_junction_op(*op),
                exprs,
            }))
        }
        Expr::BinaryExpr(binary_expr) => Ok(Expression::Binary(BinaryExpression {
            left: Box::new(to_delta_expression(binary_expr.left.as_ref())?),
            op: to_binary_op(binary_expr.op)?,
            right: Box::new(to_delta_expression(binary_expr.right.as_ref())?),
        })),
        Expr::IsNull(expr) => Ok(Expression::Unary(UnaryExpression {
            op: UnaryOperator::IsNull,
            expr: Box::new(to_delta_expression(expr.as_ref())?),
        })),
        Expr::Not(expr) => Ok(Expression::Unary(UnaryExpression {
            op: UnaryOperator::Not,
            expr: Box::new(to_delta_expression(expr.as_ref())?),
        })),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

fn datafusion_scalar_to_scalar(scalar: &ScalarValue) -> DFResult<Scalar> {
    match scalar {
        ScalarValue::Boolean(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Boolean(*value)),
            None => Ok(Scalar::Null(DataType::BOOLEAN)),
        },
        ScalarValue::Utf8(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::String(value.clone())),
            None => Ok(Scalar::Null(DataType::STRING)),
        },
        ScalarValue::Int8(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Byte(*value)),
            None => Ok(Scalar::Null(DataType::BYTE)),
        },
        ScalarValue::Int16(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Short(*value)),
            None => Ok(Scalar::Null(DataType::SHORT)),
        },
        ScalarValue::Int32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Integer(*value)),
            None => Ok(Scalar::Null(DataType::INTEGER)),
        },
        ScalarValue::Int64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Long(*value)),
            None => Ok(Scalar::Null(DataType::LONG)),
        },
        ScalarValue::Float32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Float(*value)),
            None => Ok(Scalar::Null(DataType::FLOAT)),
        },
        ScalarValue::Float64(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Double(*value)),
            None => Ok(Scalar::Null(DataType::DOUBLE)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, Some(_)) => match maybe_value {
            Some(value) => Ok(Scalar::Timestamp(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP)),
        },
        ScalarValue::TimestampMicrosecond(maybe_value, None) => match maybe_value {
            Some(value) => Ok(Scalar::TimestampNtz(*value)),
            None => Ok(Scalar::Null(DataType::TIMESTAMP_NTZ)),
        },
        ScalarValue::Date32(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Date(*value)),
            None => Ok(Scalar::Null(DataType::DATE)),
        },
        ScalarValue::Binary(maybe_value) => match maybe_value {
            Some(value) => Ok(Scalar::Binary(value.clone())),
            None => Ok(Scalar::Null(DataType::BINARY)),
        },
        ScalarValue::Decimal128(maybe_value, precision, scale) => match maybe_value {
            Some(value) => Ok(Scalar::Decimal(
                DecimalData::try_new(
                    *value,
                    DecimalType::try_new(*precision, *scale as u8).map_err(to_df_err)?,
                )
                .map_err(to_df_err)?,
            )),
            None => Ok(Scalar::Null(DataType::Primitive(PrimitiveType::Decimal(
                DecimalType::try_new(*precision, *scale as u8).map_err(to_df_err)?,
            )))),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported scalar value: {:?}",
            scalar
        ))),
    }
}

fn to_binary_op(op: Operator) -> DFResult<BinaryOperator> {
    match op {
        Operator::Eq => Ok(BinaryOperator::Equal),
        Operator::NotEq => Ok(BinaryOperator::NotEqual),
        Operator::Lt => Ok(BinaryOperator::LessThan),
        Operator::LtEq => Ok(BinaryOperator::LessThanOrEqual),
        Operator::Gt => Ok(BinaryOperator::GreaterThan),
        Operator::GtEq => Ok(BinaryOperator::GreaterThanOrEqual),
        Operator::Plus => Ok(BinaryOperator::Plus),
        Operator::Minus => Ok(BinaryOperator::Minus),
        Operator::Multiply => Ok(BinaryOperator::Multiply),
        Operator::Divide => Ok(BinaryOperator::Divide),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {:?}",
            op
        ))),
    }
}

/// Helper function to flatten nested AND/OR expressions into a single junction expression
fn flatten_junction_expr(expr: &Expr, target_op: Operator) -> DFResult<Vec<Expression>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { op, left, right }) if *op == target_op => {
            let mut left_exprs = flatten_junction_expr(left.as_ref(), target_op)?;
            let mut right_exprs = flatten_junction_expr(right.as_ref(), target_op)?;
            left_exprs.append(&mut right_exprs);
            Ok(left_exprs)
        }
        _ => {
            let delta_expr = to_delta_expression(expr)?;
            Ok(vec![delta_expr])
        }
    }
}

fn to_junction_op(op: Operator) -> JunctionOperator {
    match op {
        Operator::And => JunctionOperator::And,
        Operator::Or => JunctionOperator::Or,
        _ => unimplemented!("Unsupported operator: {:?}", op),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, lit};
    use delta_kernel::expressions::{BinaryOperator, JunctionOperator, Scalar};

    fn assert_junction_expr(expr: &Expr, expected_op: JunctionOperator, expected_children: usize) {
        let delta_expr = to_delta_expression(expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, expected_op);
                assert_eq!(junction.exprs.len(), expected_children);
            }
            _ => panic!("Expected Junction expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_simple_and() {
        let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionOperator::And, 2);
    }

    #[test]
    fn test_simple_or() {
        let expr = col("a").eq(lit(1)).or(col("b").eq(lit(2)));
        assert_junction_expr(&expr, JunctionOperator::Or, 2);
    }

    #[test]
    fn test_nested_and() {
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::And, 4);
    }

    #[test]
    fn test_nested_or() {
        let expr = col("a")
            .eq(lit(1))
            .or(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)))
            .or(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::Or, 4);
    }

    #[test]
    fn test_mixed_nested_and_or() {
        // (a AND b) OR (c AND d)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c").eq(lit(3)).and(col("d").eq(lit(4)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, JunctionOperator::Or);
                assert_eq!(junction.exprs.len(), 2);

                // Check that both children are AND junctions
                for child in junction.exprs {
                    match child {
                        Expression::Junction(child_junction) => {
                            assert_eq!(child_junction.op, JunctionOperator::And);
                            assert_eq!(child_junction.exprs.len(), 2);
                        }
                        _ => panic!("Expected Junction expression in child"),
                    }
                }
            }
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_deeply_nested_and() {
        // (((a AND b) AND c) AND d)
        let expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_junction_expr(&expr, JunctionOperator::And, 4);
    }

    #[test]
    fn test_complex_expression() {
        // (a AND b) OR ((c AND d) AND e)
        let left = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        let right = col("c")
            .eq(lit(3))
            .and(col("d").eq(lit(4)))
            .and(col("e").eq(lit(5)));
        let expr = left.or(right);

        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Junction(junction) => {
                assert_eq!(junction.op, JunctionOperator::Or);
                assert_eq!(junction.exprs.len(), 2);

                // First child should be an AND with 2 expressions
                match &junction.exprs[0] {
                    Expression::Junction(child_junction) => {
                        assert_eq!(child_junction.op, JunctionOperator::And);
                        assert_eq!(child_junction.exprs.len(), 2);
                    }
                    _ => panic!("Expected Junction expression in first child"),
                }

                // Second child should be an AND with 3 expressions
                match &junction.exprs[1] {
                    Expression::Junction(child_junction) => {
                        assert_eq!(child_junction.op, JunctionOperator::And);
                        assert_eq!(child_junction.exprs.len(), 3);
                    }
                    _ => panic!("Expected Junction expression in second child"),
                }
            }
            _ => panic!("Expected Junction expression"),
        }
    }

    #[test]
    fn test_column_expression() {
        let expr = col("test_column");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Column(name) => assert_eq!(&name.to_string(), "test_column"),
            _ => panic!("Expected Column expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_literal_expressions() {
        // Test boolean literal
        let expr = lit(true);
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Boolean(value)) => assert!(value),
            _ => panic!("Expected Boolean literal, got {:?}", delta_expr),
        }

        // Test string literal
        let expr = lit("test");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::String(value)) => assert_eq!(value, "test"),
            _ => panic!("Expected String literal, got {:?}", delta_expr),
        }

        // Test integer literal
        let expr = lit(42i32);
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 42),
            _ => panic!("Expected Integer literal, got {:?}", delta_expr),
        }

        // Test decimal literal
        let expr = lit(ScalarValue::Decimal128(Some(12345), 10, 2));
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Literal(Scalar::Decimal(data)) => {
                assert_eq!(data.bits(), 12345);
                assert_eq!(data.precision(), 10);
                assert_eq!(data.scale(), 2);
            }
            _ => panic!("Expected Decimal literal, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_binary_expressions() {
        // Test comparison operators
        let test_cases = vec![
            (col("a").eq(lit(1)), BinaryOperator::Equal),
            (col("a").not_eq(lit(1)), BinaryOperator::NotEqual),
            (col("a").lt(lit(1)), BinaryOperator::LessThan),
            (col("a").lt_eq(lit(1)), BinaryOperator::LessThanOrEqual),
            (col("a").gt(lit(1)), BinaryOperator::GreaterThan),
            (col("a").gt_eq(lit(1)), BinaryOperator::GreaterThanOrEqual),
        ];

        for (expr, expected_op) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Binary(binary) => {
                    assert_eq!(binary.op, expected_op);
                    match *binary.left {
                        Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                        _ => panic!("Expected Column expression in left operand"),
                    }
                    match *binary.right {
                        Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 1),
                        _ => panic!("Expected Integer literal in right operand"),
                    }
                }
                _ => panic!("Expected Binary expression, got {:?}", delta_expr),
            }
        }

        // Test arithmetic operators
        let test_cases = vec![
            (col("a") + lit(1), BinaryOperator::Plus),
            (col("a") - lit(1), BinaryOperator::Minus),
            (col("a") * lit(1), BinaryOperator::Multiply),
            (col("a") / lit(1), BinaryOperator::Divide),
        ];

        for (expr, expected_op) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Binary(binary) => {
                    assert_eq!(binary.op, expected_op);
                    match *binary.left {
                        Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                        _ => panic!("Expected Column expression in left operand"),
                    }
                    match *binary.right {
                        Expression::Literal(Scalar::Integer(value)) => assert_eq!(value, 1),
                        _ => panic!("Expected Integer literal in right operand"),
                    }
                }
                _ => panic!("Expected Binary expression, got {:?}", delta_expr),
            }
        }
    }

    #[test]
    fn test_unary_expressions() {
        // Test IS NULL
        let expr = col("a").is_null();
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Unary(unary) => {
                assert_eq!(unary.op, UnaryOperator::IsNull);
                match *unary.expr {
                    Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                    _ => panic!("Expected Column expression in operand"),
                }
            }
            _ => panic!("Expected Unary expression, got {:?}", delta_expr),
        }

        // Test NOT
        let expr = !col("a");
        let delta_expr = to_delta_expression(&expr).unwrap();
        match delta_expr {
            Expression::Unary(unary) => {
                assert_eq!(unary.op, UnaryOperator::Not);
                match *unary.expr {
                    Expression::Column(name) => assert_eq!(name.to_string(), "a"),
                    _ => panic!("Expected Column expression in operand"),
                }
            }
            _ => panic!("Expected Unary expression, got {:?}", delta_expr),
        }
    }

    #[test]
    fn test_null_literals() {
        let test_cases = vec![
            (lit(ScalarValue::Boolean(None)), DataType::BOOLEAN),
            (lit(ScalarValue::Utf8(None)), DataType::STRING),
            (lit(ScalarValue::Int32(None)), DataType::INTEGER),
            (lit(ScalarValue::Float64(None)), DataType::DOUBLE),
        ];

        for (expr, expected_type) in test_cases {
            let delta_expr = to_delta_expression(&expr).unwrap();
            match delta_expr {
                Expression::Literal(Scalar::Null(data_type)) => {
                    assert_eq!(data_type, expected_type);
                }
                _ => panic!("Expected Null literal, got {:?}", delta_expr),
            }
        }
    }
}
