package org.example.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

/** Запись в CSV: заголовок + маппер записи в список полей. Выход всегда UTF-8. */
public final class CsvRecordSink<T> implements RecordSink<T> {

    private final BufferedWriter writer;
    private final Function<T, List<String>> mapper;
    private long written;

    public CsvRecordSink(Path path, List<String> header, Function<T, List<String>> mapper) {
        this.mapper = mapper;
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
            writeRow(header);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void write(T record) {
        try {
            writeRow(mapper.apply(record));
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

    private void writeRow(List<String> fields) throws IOException {
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }
            writer.write(escape(fields.get(i)));
        }
        writer.newLine();
    }

    private static String escape(String value) {
        String v = value == null ? "" : value;
        if (v.indexOf(',') < 0 && v.indexOf('"') < 0 && v.indexOf('\n') < 0) {
            return v;
        }
        return '"' + v.replace("\"", "\"\"") + '"';
    }
}
