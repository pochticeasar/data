package org.example;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetRoundTripTest {

    @Test
    void writesAndReadsParquetWithoutHadoopFileSystem() throws Exception {
        Schema schema = SchemaBuilder.record("Line").fields()
                .requiredString("invoiceNo")
                .requiredInt("quantity")
                .endRecord();

        Path dir = Files.createTempDirectory("parquet-round-trip");
        Path file = dir.resolve("round-trip.parquet");
        Files.deleteIfExists(file);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(file))
                .withConf(new PlainParquetConfiguration())
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            GenericRecord r = new GenericData.Record(schema);
            r.put("invoiceNo", "536365");
            r.put("quantity", 6);
            writer.write(r);
        }

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new LocalInputFile(file), new PlainParquetConfiguration())
                .build()) {
            GenericRecord read = reader.read();
            assertEquals("536365", read.get("invoiceNo").toString());
            assertEquals(6, read.get("quantity"));
        }
    }
}
