package org.example;

import org.example.io.CsvRecordSink;
import org.example.io.RecordSink;
import org.example.query.Query;

import java.nio.file.Path;
import java.util.List;

/** Сохранение результата запроса в CSV. */
final class QueryReports {

    private QueryReports() {
    }

    static <A> void save(Path path, Query<A> query, A accumulator) {
        List<List<String>> rows = query.rows(accumulator);
        try (RecordSink<List<String>> sink = new CsvRecordSink<>(path, query.header(), row -> row)) {
            rows.forEach(sink::write);
        }
    }

    static <A> void print(Query<A> query, A accumulator, int limit) {
        List<List<String>> rows = query.rows(accumulator);
        System.out.println("  " + String.join(" | ", query.header()));
        rows.stream().limit(limit).forEach(row -> System.out.println("  " + String.join(" | ", row)));
        if (rows.size() > limit) {
            System.out.println("  ... всего строк: " + rows.size());
        }
    }
}
