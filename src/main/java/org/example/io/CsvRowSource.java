package org.example.io;

import org.example.model.RawRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Чтение Online Retail CSV. Файл не в UTF-8, по умолчанию ISO-8859-1.
 * Заголовок вычитывается в конструкторе, нумерация строк с 2 как в редакторе.
 */
public final class CsvRowSource implements RowSource {

    private final BufferedReader reader;
    private final CsvLineSplitter splitter;
    private final List<String> header;

    public CsvRowSource(Path path, Charset charset, char delimiter) {
        this.splitter = new CsvLineSplitter(delimiter);
        try {
            this.reader = Files.newBufferedReader(path, charset);
            String first = reader.readLine();
            if (first == null) {
                throw new IllegalArgumentException("Пустой файл: " + path);
            }
            this.header = splitter.split(stripBom(first));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<String> header() {
        return header;
    }

    @Override
    public Stream<RawRow> rows() {
        AtomicLong lineNumber = new AtomicLong(1);
        return reader.lines()
                .filter(line -> !line.isBlank())
                .map(line -> new RawRow(lineNumber.incrementAndGet(), splitter.split(line), line));
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String stripBom(String s) {
        return s.isEmpty() || s.charAt(0) != '﻿' ? s : s.substring(1);
    }
}
