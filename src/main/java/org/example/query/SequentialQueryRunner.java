package org.example.query;

import org.example.io.RecordSource;
import org.example.model.PreparedLine;

import java.util.function.Supplier;

/** Один поток, один проход по файлу. Базовая линия для сравнения. */
public final class SequentialQueryRunner implements QueryRunner {

    private final Supplier<RecordSource<PreparedLine>> sourceFactory;

    public SequentialQueryRunner(Supplier<RecordSource<PreparedLine>> sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

    @Override
    public <A> A run(Query<A> query) {
        A accumulator = query.newAccumulator();
        try (RecordSource<PreparedLine> source = sourceFactory.get()) {
            source.records().forEach(line -> query.accumulate(accumulator, line));
        }
        return accumulator;
    }

    @Override
    public String mode() {
        return "sequential";
    }
}
