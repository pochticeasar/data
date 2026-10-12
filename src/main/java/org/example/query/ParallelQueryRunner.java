package org.example.query;

import org.example.io.RecordSource;
import org.example.model.PreparedLine;
import org.example.parquet.ByteRange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Параллельное исполнение: один row group parquet - одна задача. Каждая задача
 * ведёт свой аккумулятор, разделяемого состояния нет, поэтому синхронизация
 * нужна только на финальном слиянии.
 */
public final class ParallelQueryRunner implements QueryRunner {

    private final List<ByteRange> ranges;
    private final Function<ByteRange, RecordSource<PreparedLine>> sourceFactory;
    private final int threads;

    public ParallelQueryRunner(
            List<ByteRange> ranges,
            Function<ByteRange, RecordSource<PreparedLine>> sourceFactory,
            int threads
    ) {
        this.ranges = List.copyOf(ranges);
        this.sourceFactory = sourceFactory;
        this.threads = Math.max(1, threads);
    }

    @Override
    public <A> A run(Query<A> query) {
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<A>> futures = new ArrayList<>(ranges.size());
            for (ByteRange range : ranges) {
                futures.add(pool.submit(() -> accumulate(query, range)));
            }

            A result = query.newAccumulator();
            for (Future<A> future : futures) {
                result = query.combine(result, future.get());
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("прогон запроса прерван", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("ошибка в задаче запроса", e.getCause());
        }
    }

    private <A> A accumulate(Query<A> query, ByteRange range) {
        A accumulator = query.newAccumulator();
        try (RecordSource<PreparedLine> source = sourceFactory.apply(range)) {
            source.records().forEach(line -> query.accumulate(accumulator, line));
        }
        return accumulator;
    }

    @Override
    public String mode() {
        return "parallel(" + threads + " потоков, " + ranges.size() + " row group)";
    }
}
