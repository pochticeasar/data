package org.example.io;

import org.example.model.RawRow;

import java.util.stream.Stream;

/** Источник сырых строк. Реализация обязана читать потоково, не держа файл в памяти. */
public interface RowSource extends AutoCloseable {

    /** Заголовок файла. */
    java.util.List<String> header();

    /** Ленивый поток строк данных без заголовка. */
    Stream<RawRow> rows();

    @Override
    void close();
}
