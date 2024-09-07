use std::sync::Arc;

use arrow::array::{Array, Datum, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn print_batch(batch: &RecordBatch) {
    for row in 0..batch.num_rows() {
        for col in 0..batch.num_columns() {
            print!("{:?}\t", batch.column(col).get());
        }
        println!();
    }
}

fn filter_batch(
    batch: &RecordBatch,
    predicate: impl Fn(i32) -> bool,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let id_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let filtered_indices: Vec<usize> = id_array
        .iter()
        .enumerate()
        .filter_map(|(i, id)| {
            if id.map(&predicate).unwrap_or(false) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    let filtered_id = Int32Array::from_iter(filtered_indices.iter().map(|&i| id_array.value(i)));
    let filtered_name =
        StringArray::from_iter(filtered_indices.iter().map(|&i| Some(name_array.value(i))));

    RecordBatch::try_new(
        batch.schema(),
        vec![Arc::new(filtered_id), Arc::new(filtered_name)],
    )
    .map_err(|e| e.into())
}

fn add_column(
    batch: &RecordBatch,
    name: &str,
    data: Vec<i32>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Int32, false)));
    let new_schema = Schema::new(fields);

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(data)) as Arc<dyn Array>);

    RecordBatch::try_new(Arc::new(new_schema), columns).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_filter_a_batch() {
        // Define the schema
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        // Create arrays
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        // Create a RecordBatch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        );
        assert!(batch.is_ok());
        let batch = batch.unwrap();

        // Print the batch
        println!("Original batch:");
        print_batch(&batch);

        // Demonstrate some operations

        // 1. Accessing a specific column
        let id_column = batch.column(0);
        println!("\nIDs: {:?}", id_column);

        // 2. Filtering data
        let filtered_batch = filter_batch(&batch, |id| id > 2).unwrap();
        println!("\nFiltered batch (id > 2):");
        print_batch(&filtered_batch);

        // 3. Adding a new column
        let new_batch = add_column(&batch, "age", vec![25, 30, 35, 40, 45]).unwrap();
        println!("\nBatch with new 'age' column:");
        print_batch(&new_batch);
    }
}
