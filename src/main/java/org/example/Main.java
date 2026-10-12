package org.example;

import org.example.ingest.IngestResult;
import org.example.prepare.PreparationResult;
import org.example.query.CustomerMetricsQuery;
import org.example.query.MonthlyRevenueQuery;
import org.example.query.Query;
import org.example.query.QueryRunner;
import org.example.query.TopProductsQuery;

import java.nio.file.Path;
import java.util.List;

/**
 * Полный прогон: ./gradlew run --args="data.csv out"
 *
 * out/raw.parquet          сырой колоночный слой, один в один с CSV
 * out/rejected_ingest.csv  строки, которые не разобрались
 * out/prepared.parquet     прошедшие правила качества строки с TotalPrice, типом и флагом возврата
 * out/rejected.csv         отбракованные правилами строки с причиной
 * out/dq_report.csv         сколько строк нарушило каждое правило
 * out/top_products.csv     топ-5 товаров
 * out/customer_metrics.csv заказы, сумма и средний чек по клиентам
 * out/monthly_revenue.csv  выручка и доля возвратов по месяцам
 */
public final class Main {

    private static final int TOP_LIMIT = 5;

    public static void main(String[] args) {
        Path input = Path.of(args.length > 0 ? args[0] : "data.csv");
        Path outDir = Path.of(args.length > 1 ? args[1] : "out");
        int threads = args.length > 2
                ? Integer.parseInt(args[2])
                : Runtime.getRuntime().availableProcessors();

        Pipeline pipeline = new Pipeline(input, outDir);

        System.out.println("== Шаг 1: загрузка CSV в parquet ==");
        long t0 = System.currentTimeMillis();
        IngestResult ingest = pipeline.ingest();
        System.out.printf("прочитано %d, записано %d, не разобрано %d, баланс %s, %d мс%n",
                ingest.read(), ingest.written(), ingest.rejected(),
                ingest.isBalanced() ? "ок" : "РАСХОЖДЕНИЕ", System.currentTimeMillis() - t0);
        ingest.parseErrors().forEach((rule, count) -> System.out.printf("  %-30s %d%n", rule, count));

        System.out.println();
        System.out.println("== Шаг 2: правила качества ==");
        t0 = System.currentTimeMillis();
        PreparationResult prepared = pipeline.prepare();
        System.out.printf("справочник товаров %d, принято %d, отбраковано %d, баланс %s, %d мс%n",
                prepared.catalogSize(), prepared.accepted(), prepared.rejected(),
                prepared.isBalanced() ? "ок" : "РАСХОЖДЕНИЕ", System.currentTimeMillis() - t0);
        prepared.violations().entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .forEach(e -> System.out.printf("  %-45s %-7s %d%n",
                        e.getKey(), prepared.severities().get(e.getKey()), e.getValue()));
        writeDqReport(outDir.resolve("dq_report.csv"), prepared);

        System.out.println();
        System.out.printf("== Шаги 3 и 4: запросы и выгрузка (%d строк, %d row group) ==%n",
                pipeline.preparedRowCount(), pipeline.preparedRowGroups());

        QueryRunner sequential = pipeline.sequentialRunner();
        QueryRunner parallel = pipeline.parallelRunner(threads);

        runAndSave(pipeline, new TopProductsQuery(TOP_LIMIT), sequential, parallel, TOP_LIMIT);
        runAndSave(pipeline, new CustomerMetricsQuery(), sequential, parallel, 5);
        runAndSave(pipeline, new MonthlyRevenueQuery(), sequential, parallel, 13);
    }

    private static <A> void runAndSave(
            Pipeline pipeline, Query<A> query, QueryRunner sequential, QueryRunner parallel, int preview) {
        long t0 = System.nanoTime();
        A sequentialResult = pipeline.query(query, sequential);
        long sequentialMs = (System.nanoTime() - t0) / 1_000_000;

        t0 = System.nanoTime();
        A parallelResult = pipeline.query(query, parallel);
        long parallelMs = (System.nanoTime() - t0) / 1_000_000;

        List<List<String>> sequentialRows = query.rows(sequentialResult);
        List<List<String>> parallelRows = query.rows(parallelResult);
        boolean same = sequentialRows.equals(parallelRows);

        System.out.println();
        System.out.printf("%s: %s %d мс, %s %d мс, ускорение x%.2f, результаты %s%n",
                query.name(), sequential.mode(), sequentialMs, parallel.mode(), parallelMs,
                parallelMs == 0 ? 0.0 : (double) sequentialMs / parallelMs,
                same ? "совпадают" : "РАСХОДЯТСЯ");

        Path saved = pipeline.save(query, parallelResult);
        QueryReports.print(query, parallelResult, preview);
        System.out.println("  -> " + saved);
    }

    private static void writeDqReport(Path path, PreparationResult result) {
        try (var sink = new org.example.io.CsvRecordSink<List<String>>(
                path, OutputFormat.REPORT_HEADER, row -> row)) {
            sink.write(List.of("rows.read", "INFO", String.valueOf(result.read())));
            sink.write(List.of("rows.accepted", "INFO", String.valueOf(result.accepted())));
            sink.write(List.of("rows.rejected", "INFO", String.valueOf(result.rejected())));
            result.violations().entrySet().stream()
                    .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                    .forEach(e -> sink.write(List.of(
                            e.getKey(), result.severities().get(e.getKey()).name(), String.valueOf(e.getValue()))));
        }
    }
}
