package org.example;

import org.example.dq.DefaultReturnPolicy;
import org.example.dq.InMemoryQualityReport;
import org.example.dq.QualityReport;
import org.example.dq.QualityRule;
import org.example.dq.Rules;
import org.example.io.CsvRecordSink;
import org.example.io.CsvRowSource;
import org.example.io.RecordSink;
import org.example.io.RowSource;
import org.example.model.InvoiceLine;
import org.example.model.PreparedLine;
import org.example.model.Rejection;
import org.example.parse.InvoiceLineParser;
import org.example.parse.RowParser;
import org.example.prepare.CatalogDescriptionTransformer;
import org.example.prepare.DataPreparation;
import org.example.prepare.LineClassifier;
import org.example.prepare.MostFrequentDescriptionCatalog;
import org.example.prepare.PreparationPipeline;
import org.example.prepare.PreparationResult;
import org.example.prepare.ProductCatalog;
import org.example.prepare.StockCodeClassifier;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Шаг 1 задания: загрузка и подготовка данных.
 * Запуск: ./gradlew run --args="data.csv out"
 *
 * На выходе три файла:
 * out/prepared.csv  - чистые строки с TotalPrice, типом позиции и флагом возврата;
 * out/rejected.csv  - всё, что не прошло правила, с причиной;
 * out/dq_report.csv - сколько строк нарушило каждое правило.
 */
public final class Main {

    private static final Charset SOURCE_CHARSET = StandardCharsets.ISO_8859_1;

    public static void main(String[] args) {
        Path input = Path.of(args.length > 0 ? args[0] : "data.csv");
        Path outDir = Path.of(args.length > 1 ? args[1] : "out");

        RowParser<InvoiceLine> parser = new InvoiceLineParser();

        long startedAt = System.currentTimeMillis();
        ProductCatalog catalog;
        try (RowSource scan = source(input)) {
            catalog = MostFrequentDescriptionCatalog.scan(scan, parser);
        }
        System.out.printf("Справочник товаров: %d кодов%n", catalog.size());

        List<QualityRule> rules = Rules.defaults();
        QualityReport report = new InMemoryQualityReport();
        LineClassifier classifier = new StockCodeClassifier();

        PreparationResult result;
        try (RecordSink<PreparedLine> prepared = new CsvRecordSink<>(
                outDir.resolve("prepared.csv"), OutputFormat.PREPARED_HEADER, OutputFormat::prepared);
             RecordSink<Rejection> rejected = new CsvRecordSink<>(
                     outDir.resolve("rejected.csv"), OutputFormat.REJECTED_HEADER, OutputFormat::rejected)) {

            DataPreparation preparation = new PreparationPipeline(
                    () -> source(input),
                    parser,
                    new CatalogDescriptionTransformer(catalog),
                    rules,
                    new DefaultReturnPolicy(),
                    classifier,
                    prepared,
                    rejected,
                    report,
                    catalog.size());
            result = preparation.run();
        }

        writeReport(outDir.resolve("dq_report.csv"), result);
        printSummary(result, System.currentTimeMillis() - startedAt);
    }

    private static RowSource source(Path input) {
        return new CsvRowSource(input, SOURCE_CHARSET, ',');
    }

    private static void writeReport(Path path, PreparationResult result) {
        try (RecordSink<List<String>> sink = new CsvRecordSink<>(
                path, OutputFormat.REPORT_HEADER, row -> row)) {
            sink.write(List.of("rows.read", "INFO", String.valueOf(result.read())));
            sink.write(List.of("rows.accepted", "INFO", String.valueOf(result.accepted())));
            sink.write(List.of("rows.rejected", "INFO", String.valueOf(result.rejected())));
            result.violations().entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .forEach(entry -> sink.write(List.of(
                            entry.getKey(),
                            result.severities().get(entry.getKey()).name(),
                            String.valueOf(entry.getValue()))));
        }
    }

    private static void printSummary(PreparationResult result, long millis) {
        System.out.printf("Прочитано: %d, принято: %d, отбраковано: %d, баланс: %s, время: %d мс%n",
                result.read(), result.accepted(), result.rejected(),
                result.isBalanced() ? "ок" : "РАСХОЖДЕНИЕ", millis);
        result.violations().entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .forEach(entry -> System.out.printf("  %-45s %-7s %d%n",
                        entry.getKey(), result.severities().get(entry.getKey()), entry.getValue()));
    }
}
