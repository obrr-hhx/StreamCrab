//! Vectorized expression evaluation over Arrow RecordBatches.
//!
//! Defines a minimal expression AST (Expr) and evaluates it against an Arrow
//! RecordBatch using Arrow compute kernels. All computation delegates to Arrow
//! so SIMD/vectorization is handled automatically.

use std::sync::Arc;

use anyhow::{bail, Result};
use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use arrow::compute::kernels::boolean::{and_kleene, not, or_kleene};
use arrow::compute::kernels::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow::compute::kernels::numeric::{
    add as arrow_add, div as arrow_div, mul as arrow_mul, sub as arrow_sub,
};
use arrow::compute::{is_not_null, is_null};
use arrow::datatypes::DataType;
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;

// ---------------------------------------------------------------------------
// Value types
// ---------------------------------------------------------------------------

/// A scalar (constant) literal value used in expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Null,
}

// ---------------------------------------------------------------------------
// Operator enums
// ---------------------------------------------------------------------------

/// Binary operators supported by the expression engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
}

/// Unary operators supported by the expression engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    IsNull,
    IsNotNull,
}

// ---------------------------------------------------------------------------
// Expression AST
// ---------------------------------------------------------------------------

/// Expression AST node evaluated over a RecordBatch.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Reference to a column by index.
    Column(usize),
    /// Constant scalar value.
    Literal(ScalarValue),
    /// Binary operation on two sub-expressions.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation on one sub-expression.
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Evaluate an expression against a RecordBatch, returning an ArrayRef.
///
/// The returned array always has `batch.num_rows()` elements.
pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        Expr::Column(idx) => {
            if *idx >= batch.num_columns() {
                bail!(
                    "column index {} out of bounds (batch has {} columns)",
                    idx,
                    batch.num_columns()
                );
            }
            Ok(batch.column(*idx).clone())
        }

        Expr::Literal(scalar) => Ok(scalar_to_array(scalar, batch.num_rows())),

        Expr::BinaryOp {
            op,
            left,
            right,
        } => {
            let l = evaluate(left, batch)?;
            let r = evaluate(right, batch)?;
            apply_binary_op(*op, &l, &r)
        }

        Expr::UnaryOp { op, expr } => {
            let arr = evaluate(expr, batch)?;
            apply_unary_op(*op, &arr)
        }
    }
}

/// Evaluate an expression and downcast the result to a BooleanArray.
///
/// Returns an error if the evaluated array is not of boolean type.
pub fn evaluate_bool(expr: &Expr, batch: &RecordBatch) -> Result<BooleanArray> {
    let arr = evaluate(expr, batch)?;
    if arr.data_type() != &DataType::Boolean {
        bail!("expected boolean result but got {:?}", arr.data_type());
    }
    Ok(arr.as_boolean().clone())
}

// ---------------------------------------------------------------------------
// Convenience builder functions
// ---------------------------------------------------------------------------

/// Build a column reference expression.
pub fn col(idx: usize) -> Expr {
    Expr::Column(idx)
}

/// Build an Int64 literal expression.
pub fn lit_i64(v: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(v))
}

/// Build a Float64 literal expression.
pub fn lit_f64(v: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(v))
}

/// Build a Bool literal expression.
pub fn lit_bool(v: bool) -> Expr {
    Expr::Literal(ScalarValue::Bool(v))
}

/// Build a Utf8 (string) literal expression.
pub fn lit_str(v: impl Into<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(v.into()))
}

/// Build `left > right`.
pub fn gt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left < right`.
pub fn lt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Lt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left >= right`.
pub fn gt_eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::GtEq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left <= right`.
pub fn lt_eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::LtEq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left == right`.
pub fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left AND right`.
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::And,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left OR right`.
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Or,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left + right`.
pub fn add(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Add,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left * right`.
pub fn mul(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Mul,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left - right`.
pub fn sub(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Sub,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build `left / right`.
pub fn div(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Div,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Expand a ScalarValue into a constant Arrow array of length `len`.
fn scalar_to_array(scalar: &ScalarValue, len: usize) -> ArrayRef {
    match scalar {
        ScalarValue::Bool(v) => Arc::new(BooleanArray::from(vec![*v; len])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; len])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v; len])),
        ScalarValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str(); len])),
        ScalarValue::Null => new_null_array(&DataType::Null, len),
    }
}

/// Dispatch a binary operator to the appropriate Arrow compute kernel.
fn apply_binary_op(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    match op {
        // --- Arithmetic ---
        BinaryOp::Add => Ok(arrow_add(left, right)?),
        BinaryOp::Sub => Ok(arrow_sub(left, right)?),
        BinaryOp::Mul => Ok(arrow_mul(left, right)?),
        BinaryOp::Div => Ok(arrow_div(left, right)?),

        // --- Comparison (produce BooleanArray) ---
        BinaryOp::Eq => Ok(Arc::new(arrow_eq(left, right)?)),
        BinaryOp::NotEq => Ok(Arc::new(arrow_neq(left, right)?)),
        BinaryOp::Lt => Ok(Arc::new(arrow_lt(left, right)?)),
        BinaryOp::LtEq => Ok(Arc::new(arrow_lt_eq(left, right)?)),
        BinaryOp::Gt => Ok(Arc::new(arrow_gt(left, right)?)),
        BinaryOp::GtEq => Ok(Arc::new(arrow_gt_eq(left, right)?)),

        // --- Logical (Kleene three-valued semantics) ---
        BinaryOp::And => {
            let l = require_boolean(left, "AND")?;
            let r = require_boolean(right, "AND")?;
            Ok(Arc::new(and_kleene(&l, &r)?))
        }
        BinaryOp::Or => {
            let l = require_boolean(left, "OR")?;
            let r = require_boolean(right, "OR")?;
            Ok(Arc::new(or_kleene(&l, &r)?))
        }
    }
}

/// Dispatch a unary operator to the appropriate Arrow compute kernel.
fn apply_unary_op(op: UnaryOp, arr: &ArrayRef) -> Result<ArrayRef> {
    match op {
        UnaryOp::Not => {
            let b = require_boolean(arr, "NOT")?;
            Ok(Arc::new(not(&b)?))
        }
        UnaryOp::IsNull => Ok(Arc::new(is_null(arr.as_ref())?)),
        UnaryOp::IsNotNull => Ok(Arc::new(is_not_null(arr.as_ref())?)),
    }
}

/// Downcast an ArrayRef to BooleanArray or bail with a descriptive error.
fn require_boolean(arr: &ArrayRef, op_name: &str) -> Result<BooleanArray> {
    if arr.data_type() != &DataType::Boolean {
        bail!(
            "{} requires a boolean array but got {:?}",
            op_name,
            arr.data_type()
        );
    }
    Ok(arr.as_boolean().clone())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    /// Build a RecordBatch with three columns:
    ///   col 0 — id    (Int64,   nullable): [1, 2, 3, null, 5]
    ///   col 1 — value (Float64, nullable): [10.0, 20.0, 30.0, 40.0, null]
    ///   col 2 — flag  (Boolean, not-null): [true, false, true, false, true]
    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(5),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(20.0),
                    Some(30.0),
                    Some(40.0),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            ],
        )
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // Column reference
    // -----------------------------------------------------------------------

    #[test]
    fn test_column_ref_returns_correct_values() {
        let batch = test_batch();
        let arr = evaluate(&col(0), &batch).unwrap();
        let ids = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.len(), 5);
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn test_column_out_of_bounds_returns_error() {
        let batch = test_batch();
        let result = evaluate(&col(99), &batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------

    #[test]
    fn test_literal_i64_broadcasts() {
        let batch = test_batch();
        let arr = evaluate(&lit_i64(42), &batch).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 5);
        assert!(ints.iter().all(|v| v == Some(42)));
    }

    #[test]
    fn test_literal_f64_broadcasts() {
        let batch = test_batch();
        let arr = evaluate(&lit_f64(3.14), &batch).unwrap();
        let floats = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.len(), 5);
        assert!((floats.value(0) - 3.14).abs() < 1e-10);
    }

    #[test]
    fn test_literal_bool_broadcasts() {
        let batch = test_batch();
        let arr = evaluate(&lit_bool(true), &batch).unwrap();
        let bools = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bools.len(), 5);
        assert!(bools.iter().all(|v| v == Some(true)));
    }

    #[test]
    fn test_literal_str_broadcasts() {
        let batch = test_batch();
        let arr = evaluate(&lit_str("hi"), &batch).unwrap();
        let strs = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strs.len(), 5);
        assert!(strs.iter().all(|v| v == Some("hi")));
    }

    // -----------------------------------------------------------------------
    // Arithmetic — Int64
    // -----------------------------------------------------------------------

    #[test]
    fn test_add_int64() {
        let batch = test_batch();
        // col(0) + 10  →  [11, 12, 13, null, 15]
        let expr = add(col(0), lit_i64(10));
        let arr = evaluate(&expr, &batch).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 11);
        assert_eq!(ints.value(1), 12);
        assert_eq!(ints.value(2), 13);
        assert!(ints.is_null(3));
        assert_eq!(ints.value(4), 15);
    }

    #[test]
    fn test_sub_int64() {
        let batch = test_batch();
        // col(0) - 1  →  [0, 1, 2, null, 4]
        let expr = sub(col(0), lit_i64(1));
        let arr = evaluate(&expr, &batch).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(4), 4);
        assert!(ints.is_null(3));
    }

    #[test]
    fn test_mul_int64() {
        let batch = test_batch();
        // col(0) * 2  →  [2, 4, 6, null, 10]
        let expr = mul(col(0), lit_i64(2));
        let arr = evaluate(&expr, &batch).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 2);
        assert_eq!(ints.value(2), 6);
        assert!(ints.is_null(3));
    }

    #[test]
    fn test_div_int64() {
        let batch = test_batch();
        // col(0) / 1  →  [1, 2, 3, null, 5]
        let expr = div(col(0), lit_i64(1));
        let arr = evaluate(&expr, &batch).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 1);
        assert_eq!(ints.value(2), 3);
        assert!(ints.is_null(3));
    }

    // -----------------------------------------------------------------------
    // Arithmetic — Float64
    // -----------------------------------------------------------------------

    #[test]
    fn test_add_float64() {
        let batch = test_batch();
        // col(1) + 5.0  →  [15.0, 25.0, 35.0, 45.0, null]
        let expr = add(col(1), lit_f64(5.0));
        let arr = evaluate(&expr, &batch).unwrap();
        let floats = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((floats.value(0) - 15.0).abs() < 1e-10);
        assert!((floats.value(3) - 45.0).abs() < 1e-10);
        assert!(floats.is_null(4));
    }

    #[test]
    fn test_mul_float64() {
        let batch = test_batch();
        // col(1) * 2.0  →  [20.0, 40.0, 60.0, 80.0, null]
        let expr = mul(col(1), lit_f64(2.0));
        let arr = evaluate(&expr, &batch).unwrap();
        let floats = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((floats.value(0) - 20.0).abs() < 1e-10);
        assert!((floats.value(2) - 60.0).abs() < 1e-10);
        assert!(floats.is_null(4));
    }

    // -----------------------------------------------------------------------
    // Comparison operators
    // -----------------------------------------------------------------------

    #[test]
    fn test_eq_int64() {
        let batch = test_batch();
        // col(0) == 3  →  [false, false, true, null, false]
        let expr = eq(col(0), lit_i64(3));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(2), true);
        assert!(result.is_null(3));
        assert_eq!(result.value(4), false);
    }

    #[test]
    fn test_neq_int64() {
        let batch = test_batch();
        let expr = Expr::BinaryOp {
            op: BinaryOp::NotEq,
            left: Box::new(col(0)),
            right: Box::new(lit_i64(3)),
        };
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(2), false);
        assert!(result.is_null(3));
    }

    #[test]
    fn test_lt_int64() {
        let batch = test_batch();
        // col(0) < 3  →  [true, true, false, null, false]
        let expr = lt(col(0), lit_i64(3));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(1), true);
        assert_eq!(result.value(2), false);
        assert!(result.is_null(3));
        assert_eq!(result.value(4), false);
    }

    #[test]
    fn test_lt_eq_int64() {
        let batch = test_batch();
        let expr = lt_eq(col(0), lit_i64(3));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(2), true);
        assert_eq!(result.value(4), false);
    }

    #[test]
    fn test_gt_int64() {
        let batch = test_batch();
        let expr = gt(col(0), lit_i64(3));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(2), false);
        assert_eq!(result.value(4), true);
    }

    #[test]
    fn test_gt_eq_int64() {
        let batch = test_batch();
        let expr = gt_eq(col(0), lit_i64(3));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(2), true);
        assert_eq!(result.value(4), true);
    }

    // -----------------------------------------------------------------------
    // Logical operators
    // -----------------------------------------------------------------------

    #[test]
    fn test_and_kleene() {
        let batch = test_batch();
        // col(2): [T, F, T, F, T]  AND true  →  [T, F, T, F, T]
        let expr = and(col(2), lit_bool(true));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(1), false);
        assert_eq!(result.value(4), true);
    }

    #[test]
    fn test_or_kleene() {
        let batch = test_batch();
        // col(2) OR false  →  same as col(2)
        let expr = or(col(2), lit_bool(false));
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(1), false);
        assert_eq!(result.value(2), true);
    }

    #[test]
    fn test_not() {
        let batch = test_batch();
        // NOT col(2): [F, T, F, T, F]
        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(col(2)),
        };
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(1), true);
        assert_eq!(result.value(3), true);
        assert_eq!(result.value(4), false);
    }

    #[test]
    fn test_and_on_non_boolean_returns_error() {
        let batch = test_batch();
        let expr = and(col(0), col(0));
        assert!(evaluate(&expr, &batch).is_err());
    }

    // -----------------------------------------------------------------------
    // IsNull / IsNotNull
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_null() {
        let batch = test_batch();
        // col(0) IS NULL: [F, F, F, T, F]
        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNull,
            expr: Box::new(col(0)),
        };
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(3), true);
        assert_eq!(result.value(4), false);
    }

    #[test]
    fn test_is_not_null() {
        let batch = test_batch();
        // col(1) IS NOT NULL: [T, T, T, T, F]
        let expr = Expr::UnaryOp {
            op: UnaryOp::IsNotNull,
            expr: Box::new(col(1)),
        };
        let result = evaluate_bool(&expr, &batch).unwrap();
        assert_eq!(result.value(0), true);
        assert_eq!(result.value(3), true);
        assert_eq!(result.value(4), false);
    }

    // -----------------------------------------------------------------------
    // Nested expressions
    // -----------------------------------------------------------------------

    #[test]
    fn test_nested_gt_and_lt() {
        let batch = test_batch();
        // (col(0) > 1) AND (col(1) < 35.0)
        //   col(0) > 1:    [F,  T,  T,  null, T   ]
        //   col(1) < 35.0: [T,  T,  T,  F,    null]
        //   Kleene AND:    [F,  T,  T,  F,     null]
        //     row3: null AND false = false  (false dominates)
        //     row4: true AND null  = null
        let result = evaluate_bool(&and(gt(col(0), lit_i64(1)), lt(col(1), lit_f64(35.0))), &batch).unwrap();
        assert_eq!(result.value(0), false);
        assert_eq!(result.value(1), true);
        assert_eq!(result.value(2), true);
        // row3: null AND false = false (Kleene)
        assert!(!result.value(3) || result.is_null(3));
        // row4: true AND null = null (Kleene)
        assert!(result.is_null(4));
    }

    #[test]
    fn test_nested_arithmetic_then_compare() {
        let batch = test_batch();
        // (col(0) * 2) > 5  →  [F, F, T, null, T]
        let result = evaluate_bool(&gt(mul(col(0), lit_i64(2)), lit_i64(5)), &batch).unwrap();
        assert_eq!(result.value(0), false); // 2 > 5
        assert_eq!(result.value(1), false); // 4 > 5
        assert_eq!(result.value(2), true);  // 6 > 5
        assert!(result.is_null(3));         // null
        assert_eq!(result.value(4), true);  // 10 > 5
    }

    #[test]
    fn test_evaluate_bool_rejects_non_boolean() {
        let batch = test_batch();
        let result = evaluate_bool(&col(0), &batch);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("boolean"));
    }
}
