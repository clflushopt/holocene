//! Implementation of physical operators based on Datafusion's `PhysicalExpr` type to represent
//! expressions.

use datafusion::{
    arrow::{
        array::AsArray, compute::filter_record_batch, datatypes::Schema, record_batch::RecordBatch,
    },
    physical_plan::PhysicalExpr,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum Result {
    Ok(Arc<RecordBatch>),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Scan,
    Filter,
    Project,
    Limit,
    Sort,
    HashAggregate,
    HashJoinBuild,
    HashJoinProbe,
}

/// Physical operator is used to unify the notion of an operator while at the same time
/// differentiating between pipeline breaking operators (also called sink operators) and
/// non-pipeline breaking operators.
trait PhysicalOperator {
    /// Returns the operator kind.
    fn kind(&self) -> Kind;
}

/// PipelinedOperator defines an operator that can be pipelined anywhere in the query plan
/// as such its not a pipeline breaking operator.
trait PipelinedOperator: PhysicalOperator {
    /// Execute the operator logic on the batch returning a result.
    fn execute(&mut self, batch: Arc<RecordBatch>) -> Result;
}

/// SinkOperator defines a pipeline breaking operator.
trait SinkOperator: PhysicalOperator + PipelinedOperator {
    ///Finalize the pipeline by emitting the last record batch.
    fn finalize(&self) -> Vec<Arc<RecordBatch>>;
}

///Filter applies a predicate expression on values pushing out only those that evaluate
/// to `true`.
#[derive(Debug, Clone)]
struct Filter {
    expr: Arc<dyn PhysicalExpr>,
    schema: Arc<Schema>,
}

impl Filter {
    pub fn new(expr: Arc<dyn PhysicalExpr>, schema: Arc<Schema>) -> Self {
        Self { expr, schema }
    }
}

impl PhysicalOperator for Filter {
    fn kind(&self) -> Kind {
        Kind::Filter
    }
}

impl PipelinedOperator for Filter {
    fn execute(&mut self, batch: Arc<RecordBatch>) -> Result {
        let preds = match self.expr.evaluate(batch.as_ref()) {
            Ok(result) => result.into_array(batch.num_rows()).unwrap(),
            Err(issue) => panic!("Couldn't evaluate filter on batch {issue}"),
        };
        match filter_record_batch(batch.as_ref(), preds.as_boolean()) {
            Ok(records) => Result::Ok(Arc::new(records)),
            Err(issue) => panic!("Couldn't apply compute primitive on records due to {issue}"),
        }
    }
}

/// Scan operator reads data from a source and produces RecordBatches
#[derive(Debug, Clone)]
struct Scan {
    schema: Arc<Schema>,
    batches: Vec<Arc<RecordBatch>>,
    current_batch: usize,
}

impl Scan {
    pub fn new(schema: Arc<Schema>, batches: Vec<Arc<RecordBatch>>) -> Self {
        Self {
            schema,
            batches,
            current_batch: 0,
        }
    }
}

impl PhysicalOperator for Scan {
    fn kind(&self) -> Kind {
        Kind::Scan
    }
}

impl PipelinedOperator for Scan {
    fn execute(&mut self, _: Arc<RecordBatch>) -> Result {
        if self.current_batch < self.batches.len() {
            let batch = Arc::clone(&self.batches[self.current_batch]);
            self.current_batch += 1;
            Result::Ok(batch)
        } else {
            Result::Done
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::str::FromStr;

    use super::*;
    use datafusion::arrow::array::{Float64Array, Int32Array, Scalar};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::Column;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    #[test]
    fn scan_operator() {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // Create some test data
        let batch1 = Arc::new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
                ],
            )
            .unwrap(),
        );

        let batch2 = Arc::new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(Float64Array::from(vec![4.4, 5.5, 6.6])),
                ],
            )
            .unwrap(),
        );

        // Create a Scan operator
        let mut scan = Scan::new(schema, vec![batch1, batch2]);

        // Test execution
        match scan.execute(Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty())))) {
            Result::Ok(batch) => {
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.num_rows(), 3);
                let id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id_array.value(0), 1);
                assert_eq!(id_array.value(2), 3);
            }
            _ => panic!("Expected Done result"),
        }

        match scan.execute(Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty())))) {
            Result::Ok(batch) => {
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.num_rows(), 3);
                let id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(id_array.value(0), 4);
                assert_eq!(id_array.value(2), 6);
            }
            _ => panic!("Expected Done result"),
        }

        match scan.execute(Arc::new(RecordBatch::new_empty(Arc::new(Schema::empty())))) {
            Result::Done => {}
            _ => panic!("Expected Finished result"),
        }
    }

    #[test]
    fn filter_operator() {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // Create test data
        let batch = Arc::new(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5])),
                ],
            )
            .unwrap(),
        );

        // Create a Filter operator with a "greater than 2" condition on the "id" column
        let lhs = Arc::new(expressions::Column::new("id", 0));
        let rhs = Arc::new(expressions::Literal::new(ScalarValue::Int32(Some(2))));
        let operator = Operator::Gt;
        let expr = datafusion::physical_expr::expressions::BinaryExpr::new(lhs, operator, rhs);
        let mut filter = Filter::new(Arc::new(expr), Arc::clone(&schema));

        // Execute the filter
        match filter.execute(Arc::clone(&batch)) {
            Result::Ok(filtered_batch) => {
                assert_eq!(filtered_batch.num_columns(), 2);
                assert_eq!(filtered_batch.num_rows(), 3); // Should have filtered out values <= 2

                let id_array = filtered_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let value_array = filtered_batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                // Check that only values > 2 remain
                assert_eq!(id_array.value(0), 3);
                assert_eq!(id_array.value(1), 4);
                assert_eq!(id_array.value(2), 5);

                assert_eq!(value_array.value(0), 3.3);
                assert_eq!(value_array.value(1), 4.4);
                assert_eq!(value_array.value(2), 5.5);
            }
            _ => panic!("Expected Done result"),
        }
    }
}
