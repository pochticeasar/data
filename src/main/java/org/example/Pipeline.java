package org.example;

import org.example.dq.DefaultReturnPolicy;
import org.example.dq.InMemoryQualityReport;
import org.example.dq.QualityReport;
import org.example.dq.Rules;
import org.example.ingest.CsvToParquetIngestion;
import org.example.ingest.DataIngestion;
import org.example.ingest.IngestResult;
import org.example.io.CsvRecordSink;
import org.example.io.CsvRowSource;
import org.example.io.RecordSink;
import org.example.io.RecordSource;
import org.example.io.RowSource;
import org.example.model.InvoiceLine;
import org.example.model.PreparedLine;
import org.example.model.Rejection;
import org.example.parquet.ByteRange;
import org.example.parquet.ParquetRecordSink;
import org.example.parquet.ParquetRecordSource;
import org.example.parquet.PreparedLineMapper;
import org.example.parquet.RawLineMapper;
import org.example.parquet.RowGroups;
import org.example.parse.InvoiceLineParser;
import org.example.parse.RowParser;
import org.example.prepare.CatalogDescriptionTransformer;
import org.example.prepare.DataPreparation;
import org.example.prepare.MostFrequentDescriptionCatalog;
import org.example.prepare.PreparationPipeline;
import org.example.prepare.PreparationResult;
import org.example.prepare.ProductCatalog;
import org.example.prepare.StockCodeClassifier;
import org.example.query.ParallelQueryRunner;
import org.example.query.Query;
import org.example.query.QueryRunner;
import org.example.query.SequentialQueryRunner;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Склейка четырёх шагов: загрузка в parquet, обработка качества, запросы, выгрузка.
 * Каждый шаг читает файл предыдущего, поэтому их можно запускать и по отдельности.
 */
public final class Pipeline {

    private static final Charset SOURCE_CHARSET = StandardCharsets.ISO_8859_1;
    private static final char DELIMITER = ',';

    private final Path input;
    private final Path outDir;

    public Pipeline(Path input, Path outDir) {
        this.input = input;
        this.outDir = outDir;
    }

    public Path rawParquet() {
        return outDir.resolve("raw.parquet");
    }

    public Path preparedParquet() {
        return outDir.resolve("prepared.parquet");
    }

    /** Шаг 1: CSV -> raw.parquet. */
    public IngestResult ingest() {
        RowParser<InvoiceLine> parser = new InvoiceLineParser();
        try (RecordSink<InvoiceLine> raw = new ParquetRecordSink<>(
                rawParquet(), new RawLineMapper(), ParquetRecordSink.DEFAULT_ROW_GROUP_BYTES);
             RecordSink<Rejection> rejected = new CsvRecordSink<>(
                     outDir.resolve("rejected_ingest.csv"), OutputFormat.REJECTED_HEADER, OutputFormat::rejected)) {

            DataIngestion ingestion = new CsvToParquetIngestion(this::csvSource, parser, raw, rejected);
            return ingestion.run();
        }
    }

    /** Шаг 2: raw.parquet -> prepared.parquet, rejected.csv, dq_report.csv. */
    public PreparationResult prepare() {
        ProductCatalog catalog;
        try (RecordSource<InvoiceLine> scan = rawSource()) {
            catalog = MostFrequentDescriptionCatalog.scan(scan);
        }

        QualityReport report = new InMemoryQualityReport();
        try (RecordSink<PreparedLine> prepared = new ParquetRecordSink<>(
                preparedParquet(), new PreparedLineMapper(), ParquetRecordSink.PARALLEL_ROW_GROUP_BYTES);
             RecordSink<Rejection> rejected = new CsvRecordSink<>(
                     outDir.resolve("rejected.csv"), OutputFormat.REJECTED_HEADER, OutputFormat::rejected)) {

            DataPreparation preparation = new PreparationPipeline(
                    this::rawSource,
                    new CatalogDescriptionTransformer(catalog),
                    Rules.defaults(),
                    new DefaultReturnPolicy(),
                    new StockCodeClassifier(),
                    prepared,
                    rejected,
                    report,
                    catalog.size());
            return preparation.run();
        }
    }

    /** Шаг 3: запрос по подготовленному слою. */
    public <A> A query(Query<A> query, QueryRunner runner) {
        return runner.run(query);
    }

    /** Шаг 4: сохранение результата. */
    public <A> Path save(Query<A> query, A accumulator) {
        Path path = outDir.resolve(query.name() + ".csv");
        QueryReports.save(path, query, accumulator);
        return path;
    }

    public QueryRunner sequentialRunner() {
        return new SequentialQueryRunner(this::preparedSource);
    }

    public QueryRunner parallelRunner(int threads) {
        List<ByteRange> ranges = RowGroups.ranges(preparedParquet());
        return new ParallelQueryRunner(ranges, this::preparedSource, threads);
    }

    public long preparedRowCount() {
        return RowGroups.rowCount(preparedParquet());
    }

    public int preparedRowGroups() {
        return RowGroups.ranges(preparedParquet()).size();
    }

    private RowSource csvSource() {
        return new CsvRowSource(input, SOURCE_CHARSET, DELIMITER);
    }

    private RecordSource<InvoiceLine> rawSource() {
        return new ParquetRecordSource<>(rawParquet(), new RawLineMapper());
    }

    private RecordSource<PreparedLine> preparedSource() {
        return new ParquetRecordSource<>(preparedParquet(), new PreparedLineMapper());
    }

    private RecordSource<PreparedLine> preparedSource(ByteRange range) {
        return new ParquetRecordSource<>(preparedParquet(), new PreparedLineMapper(), range);
    }
}
