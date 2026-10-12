package org.example.io;

import java.util.stream.Stream;

/** Источник готовых записей. Поток ленивый, источник закрывается вызывающим. */
public interface RecordSource<T> extends AutoCloseable {

    Stream<T> records();

    @Override
    void close();
}
