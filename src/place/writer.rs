use super::Place;
use anyhow::{Ok, Result};
use arrow::{
    array::{Int32Array, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub struct ParquetWriter {
    schema: Arc<Schema>,
    writer: ArrowWriter<File>,
    places: Vec<Place>,
}

impl ParquetWriter {
    pub fn try_new(batch_size: usize, out: &Path) -> Result<ParquetWriter> {
        assert!(batch_size > 0);
        let file = File::create(out)?;
        let schema = Arc::new(make_schema());
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(15)?))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        Ok(ParquetWriter {
            schema,
            writer,
            places: Vec::with_capacity(batch_size),
        })
    }

    pub fn write(&mut self, place: Place) -> Result<()> {
        if self.places.len() == self.places.capacity() {
            self.flush()?;
        }
        self.places.push(place);
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let s2_cell_ids = Arc::new(UInt64Array::from_iter(
            self.places.iter().map(|p| p.s2_cell_id),
        ));
        let longitudes = Arc::new(Int32Array::from_iter(self.places.iter().map(|p| p.lon_e7)));
        let latitudes = Arc::new(Int32Array::from_iter(self.places.iter().map(|p| p.lat_e7)));
        let source_ids = Arc::new(StringArray::from_iter_values(
            self.places.iter().map(|p| p.source_id.as_str()),
        ));
        let source_urls = Arc::new(StringArray::from_iter(
            self.places.iter().map(|p| p.source_url.as_deref()),
        ));
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![s2_cell_ids, longitudes, latitudes, source_ids, source_urls],
        )?;
        self.writer.write(&batch)?;
        self.places.clear();
        Ok(())
    }
}

fn make_schema() -> Schema {
    Schema::new(vec![
        Field::new("s2_cell_id", DataType::UInt64, /* nullable */ false),
        Field::new("longitude_e7", DataType::Int32, /* nullable */ false),
        Field::new("latitude_e7", DataType::Int32, /* nullable */ false),
        Field::new("source_id", DataType::Utf8, /* nullable */ false),
        Field::new("source_url", DataType::Utf8, /* nullable */ true),
        /*
        Field::new(
                "tags",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(vec![
                            Field::new("key", DataType::Utf8, /* nullable */ false),
                            Field::new("value", DataType::Utf8, /* nullable */ false),
                        ].into()),
                        /* nullable */ false,
                    )),
                    /* sorted */ true,
                ),
                false, // non-nullable
        ),
        */
    ])
}
