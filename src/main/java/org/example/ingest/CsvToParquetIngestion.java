package org.example.ingest;

import org.example.io.RecordSink;
import org.example.io.RowSource;
import org.example.model.InvoiceLine;
import org.example.model.Rejection;
import org.example.parse.ParseResult;
import org.example.parse.RowParser;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Загрузка CSV в parquet. На этом шаге отбраковываются только технически битые
 * строки: не то число полей, не парсится число или дата. Всё остальное едет
 * в сырой слой как есть, чтобы правила качества можно было менять и прогонять
 * заново без повторного разбора CSV.
 */
public final class CsvToParquetIngestion implements DataIngestion {

    private final Supplier<RowSource> sourceFactory;
    private final RowParser<InvoiceLine> parser;
    private final RecordSink<InvoiceLine> rawSink;
    private final RecordSink<Rejection> rejectedSink;

    public CsvToParquetIngestion(
            Supplier<RowSource> sourceFactory,
            RowParser<InvoiceLine> parser,
            RecordSink<InvoiceLine> rawSink,
            RecordSink<Rejection> rejectedSink
    ) {
        this.sourceFactory = sourceFactory;
        this.parser = parser;
        this.rawSink = rawSink;
        this.rejectedSink = rejectedSink;
    }

    @Override
    public IngestResult run() {
        Map<String, Long> parseErrors = new LinkedHashMap<>();
        long[] counters = new long[2];

        try (RowSource source = sourceFactory.get()) {
            source.rows().forEach(row -> {
                switch (parser.parse(row)) {
                    case ParseResult.Ok<InvoiceLine> ok -> {
                        rawSink.write(ok.value());
                        counters[0]++;
                    }
                    case ParseResult.Error<InvoiceLine> error -> {
                        rejectedSink.write(new Rejection(
                                row.lineNumber(), error.rule(), error.message(), row.rawLine()));
                        parseErrors.merge(error.rule(), 1L, Long::sum);
                        counters[1]++;
                    }
                }
            });
        }
        return new IngestResult(counters[0] + counters[1], counters[0], counters[1], parseErrors);
    }
}
