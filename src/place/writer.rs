use anyhow::{Ok, Result};
use arrow::datatypes::{DataType, Field, Schema};
use std::path::Path;
use std::sync::Arc;

use super::Place;

pub struct ParquetWriter {}

impl ParquetWriter {
    pub fn new(_out: &Path) -> Result<ParquetWriter> {
        let _schema = make_parquet_schema();
        Ok(ParquetWriter {})
    }

    pub fn write(&mut self, _place: Place) -> Result<()> {
        Ok(())
    }
}

fn make_parquet_schema() -> Schema {
    // Metadata for GEOMETRY logical type
    //
    // let mut point_metadata = HashMap::new();
    // point_metadata.insert(
    //     "ARROW:extension:metadata".to_string(),
    //     r#"{"logicalType": "GEOMETRY"}"#.to_string(),
    // );
    Schema::new(vec![
        // Field::new("point", DataType::FixedSizeBinary(point_size as i32), false)
        //     .with_metadata(point_metadata),
        Field::new(
            "tags",
            DataType::Map(Arc::new(Field::new("entries", DataType::Utf8, true)), false),
            false,
        ),
        Field::new("spider", DataType::Utf8, false),
    ])
}
