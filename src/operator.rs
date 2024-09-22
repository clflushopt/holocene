//! Implementation of physical operators based on Datafusion's `PhysicalExpr` type to represent
//! expressions.

use datafusion::{
    arrow::{
        array::{ArrayRef, AsArray},
        compute::{
            concat_batches, filter_record_batch, lexsort_to_indices, sort_to_indices, take,
            SortOptions,
        },
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{sorts::sort::sort_batch, PhysicalExpr},
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum Result {
    Ok(Arc<RecordBatch>),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Source,
    Scan,
    Filter,
    Projection,
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

/// Projection operator.
struct Projection {
    child: Box<dyn PipelinedOperator>,
    indices: Vec<usize>,
    output_schema: SchemaRef,
}

impl Projection {
    fn new(
        child: Box<dyn PipelinedOperator>,
        indices: Vec<usize>,
        input_schema: SchemaRef,
    ) -> Self {
        let projected_fields: Vec<_> = indices
            .iter()
            .map(|&i| input_schema.field(i).clone())
            .collect();
        let output_schema = Arc::new(Schema::new(projected_fields));

        Self {
            child,
            indices,
            output_schema,
        }
    }
}

impl PhysicalOperator for Projection {
    fn kind(&self) -> Kind {
        Kind::Projection
    }
}

impl PipelinedOperator for Projection {
    fn execute(&mut self, batch: Arc<RecordBatch>) -> Result {
        let input_batch = match self.child.execute(batch) {
            Result::Ok(batch) => batch,
            Result::Done => return Result::Done,
        };

        let projected_columns: Vec<ArrayRef> = self
            .indices
            .iter()
            .map(|&i| input_batch.column(i).clone())
            .collect();

        let projected_batch = RecordBatch::try_new(self.output_schema.clone(), projected_columns)
            .map_err(|e| format!("Error creating projected batch: {}", e));

        match projected_batch {
            Ok(batch) => Result::Ok(Arc::new(batch)),
            Err(issue) => panic!("Encountered an issue building the last batch {issue}"),
        }
    }
}

// Limit operator.
struct Limit {
    child: Box<dyn PipelinedOperator>,
    limit: usize,
    emitted: usize,
}

impl Limit {
    fn new(child: Box<dyn PipelinedOperator>, limit: usize) -> Self {
        Self {
            child,
            limit,
            emitted: 0,
        }
    }
}

impl PhysicalOperator for Limit {
    fn kind(&self) -> Kind {
        Kind::Limit
    }
}

impl PipelinedOperator for Limit {
    fn execute(&mut self, batch: Arc<RecordBatch>) -> Result {
        if self.emitted >= self.limit {
            return Result::Done;
        }

        let input_batch = match self.child.execute(batch) {
            Result::Ok(batch) => batch,
            Result::Done => return Result::Done,
        };
        let remaining = self.limit - self.emitted;
        let output_rows = std::cmp::min(remaining, input_batch.num_rows());

        let limited_columns: Vec<ArrayRef> = input_batch
            .columns()
            .iter()
            .map(|col| col.slice(0, output_rows))
            .collect();

        let limited_batch = RecordBatch::try_new(input_batch.schema(), limited_columns)
            .map_err(|e| format!("Error creating limited batch: {}", e));

        match limited_batch {
            Ok(batch) => {
                self.emitted += output_rows;
                println!(
                    "Emitted: {}, Batch Size: {}",
                    self.emitted,
                    batch.num_rows()
                );

                if self.emitted >= self.limit {
                    if batch.num_rows() > 0 {
                        return Result::Ok(Arc::new(batch));
                    }
                    Result::Done
                } else {
                    Result::Ok(Arc::new(batch))
                }
            }
            Err(issue) => panic!("Encountered an issue {issue}"),
        }
    }
}

pub struct Sort {
    expr: Vec<PhysicalSortExpr>,
    originals: Vec<RecordBatch>,
}

impl Sort {
    pub fn new(expr: Vec<PhysicalSortExpr>) -> Self {
        Self {
            expr,
            originals: Vec::new(),
        }
    }
}

impl PhysicalOperator for Sort {
    fn kind(&self) -> Kind {
        Kind::Sort
    }
}

impl PipelinedOperator for Sort {
    fn execute(&mut self, batch: Arc<RecordBatch>) -> Result {
        self.originals.push(batch.as_ref().clone());
        Result::Done
    }
}

impl SinkOperator for Sort {
    fn finalize(&self) -> Vec<Arc<RecordBatch>> {
        let combined = concat_batches(&self.originals[0].schema(), self.originals.iter()).unwrap();

        let sort_columns = self
            .expr
            .iter()
            .map(|expr| expr.evaluate_to_sort_column(&combined).unwrap())
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices(&sort_columns, None).unwrap();

        let columns = combined
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<std::result::Result<_, _>>()
            .unwrap();

        let batch = RecordBatch::try_new(combined.schema(), columns).unwrap();
        let size = 32;
        let num_output_vec = (batch.num_rows() + size - 1) / size;
        let mut output_vec = Vec::with_capacity(num_output_vec);
        let mut output_offset: usize = 0;
        loop {
            if output_offset >= batch.num_rows() {
                return output_vec;
            }
            let length = std::cmp::min(1024, batch.num_rows() - output_offset);
            output_vec.push(Arc::new(batch.slice(output_offset, length)));
            output_offset += length;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions;
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

    struct TestSource {
        data: Vec<Arc<RecordBatch>>,
        index: usize,
    }

    impl TestSource {
        fn new(data: Vec<Arc<RecordBatch>>) -> Self {
            Self { data, index: 0 }
        }
    }

    impl PhysicalOperator for TestSource {
        fn kind(&self) -> Kind {
            Kind::Source
        }
    }

    impl PipelinedOperator for TestSource {
        fn execute(&mut self, _: Arc<RecordBatch>) -> Result {
            if self.index < self.data.len() {
                let batch = self.data[self.index].clone();
                self.index += 1;
                Result::Ok(batch)
            } else {
                Result::Done
            }
        }
    }

    #[test]
    fn projection_operator() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5])),
                ],
            )
            .unwrap(),
        );

        let source = Box::new(TestSource::new(vec![batch]));
        let mut projection = Projection::new(source, vec![1], schema.clone());

        match projection.execute(Arc::new(RecordBatch::new_empty(schema))) {
            Result::Ok(projected_batch) => {
                assert_eq!(projected_batch.num_columns(), 1);
                assert_eq!(projected_batch.num_rows(), 5);
                let value_array = projected_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(value_array.value(0), 1.1);
                assert_eq!(value_array.value(4), 5.5);
            }
            _ => panic!("Expected Done result"),
        }
    }

    #[test]
    fn limit_operator() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch1 = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        );

        let batch2 = Arc::new(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
            )
            .unwrap(),
        );

        let source = Box::new(TestSource::new(vec![batch1, batch2]));
        let mut limit = Limit::new(source, 4);

        let mut total_rows = 0;
        loop {
            match limit.execute(Arc::new(RecordBatch::new_empty(schema.clone()))) {
                Result::Ok(limited_batch) => {
                    println!("Limit.execute => {}", limited_batch.num_rows());
                    total_rows += limited_batch.num_rows();
                }
                Result::Done => break,
                _ => panic!("Unexpected result"),
            }
        }

        assert_eq!(total_rows, 4);
    }

    #[test]
    fn sort_operator() {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create some test data
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 4])),
                Arc::new(StringArray::from(vec!["c", "a", "d"])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![5, 2])),
                Arc::new(StringArray::from(vec!["e", "b"])),
            ],
        )
        .unwrap();

        // Create sort expressions
        let sort_expr: Vec<PhysicalSortExpr> = vec![PhysicalSortExpr {
            // expr: col("id", &schema).unwrap(),
            expr: Arc::new(expressions::Column::new("id", 0)),
            options: SortOptions::default(),
        }];

        // Create and execute the Sort operator
        let mut sort = Sort::new(sort_expr);
        sort.execute(Arc::new(batch1));
        sort.execute(Arc::new(batch2));

        // Finalize and get the sorted result
        let result = sort.finalize();

        // Verify the result
        assert_eq!(result.len(), 1, "Expected 1 output batch");
        let sorted_batch = result[0].as_ref();
        assert_eq!(sorted_batch.num_rows(), 5, "Expected 5 rows in total");

        // Check if the 'id' column is sorted
        let id_array = sorted_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sorted_ids: Vec<i32> = id_array.iter().map(|v| v.unwrap()).collect();
        assert_eq!(sorted_ids, vec![1, 2, 3, 4, 5], "IDs should be sorted");

        // Check if the 'name' column is correctly ordered
        let name_array = sorted_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sorted_names: Vec<&str> = name_array.iter().map(|v| v.unwrap()).collect();
        assert_eq!(
            sorted_names,
            vec!["a", "b", "c", "d", "e"],
            "Names should be in corresponding order"
        );
    }
}
