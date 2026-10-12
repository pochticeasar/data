package org.example.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.example.io.RecordSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Чтение parquet. Диапазон байт ограничивает чтение подмножеством row group:
 * берутся те, чьё начало попало в [start, end). Так файл делится между потоками
 * без пересечений и без пропусков.
 */
public final class ParquetRecordSource<T> implements RecordSource<T> {

    private final ParquetReader<GenericRecord> reader;
    private final ParquetMapper<T> mapper;

    public ParquetRecordSource(Path path, ParquetMapper<T> mapper) {
        this(path, mapper, null);
    }

    public ParquetRecordSource(Path path, ParquetMapper<T> mapper, ByteRange range) {
        this.mapper = mapper;
        try {
            ParquetReader.Builder<GenericRecord> builder = AvroParquetReader
                    .<GenericRecord>builder(new LocalInputFile(path), new PlainParquetConfiguration());
            if (range != null) {
                builder = builder.withFileRange(range.start(), range.end());
            }
            this.reader = builder.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Stream<T> records() {
        Iterator<T> iterator = new Iterator<>() {
            private T next = advance();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public T next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                T current = next;
                next = advance();
                return current;
            }

            private T advance() {
                try {
                    GenericRecord record = reader.read();
                    return record == null ? null : mapper.fromRecord(record);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
