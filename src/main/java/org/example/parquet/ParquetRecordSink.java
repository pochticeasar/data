package org.example.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.example.io.RecordSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Запись в parquet со сжатием snappy. Размер row group задаёт гранулярность
 * параллельного чтения: один row group - одна задача.
 */
public final class ParquetRecordSink<T> implements RecordSink<T> {

    public static final int DEFAULT_ROW_GROUP_BYTES = 16 * 1024 * 1024;

    /**
     * Датасет маленький: при обычных 128 МБ на row group весь слой лёг бы в один блок
     * и параллелить было бы нечего. Для подготовленного слоя берём 1 МБ, чтобы
     * получить десяток блоков и показать масштабирование. На продовых объёмах
     * так делать не надо: мелкие row group съедают эффект колоночного сжатия.
     */
    public static final int PARALLEL_ROW_GROUP_BYTES = 1024 * 1024;

    private final ParquetWriter<GenericRecord> writer;
    private final ParquetMapper<T> mapper;
    private long written;

    public ParquetRecordSink(Path path, ParquetMapper<T> mapper, int rowGroupBytes) {
        this.mapper = mapper;
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            Files.deleteIfExists(path);
            this.writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(path))
                    .withConf(new PlainParquetConfiguration())
                    .withSchema(mapper.schema())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize((long) rowGroupBytes)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(T record) {
        try {
            writer.write(mapper.toRecord(record));
            written++;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long written() {
        return written;
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
