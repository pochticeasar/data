package org.example.io;

/** Приёмник записей на выход. */
public interface RecordSink<T> extends AutoCloseable {

    void write(T record);

    long written();

    @Override
    void close();
}
