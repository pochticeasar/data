package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Двусторонний маппинг записи приложения в avro-запись parquet. */
public interface ParquetMapper<T> {

    Schema schema();

    GenericRecord toRecord(T value);

    T fromRecord(GenericRecord record);
}
