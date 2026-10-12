package org.example;

import org.apache.spark.sql.SparkSession;
import org.example.spark.CustomerMetricsQuery;
import org.example.spark.MonthlyRevenueQuery;
import org.example.spark.SparkPipeline;
import org.example.spark.SparkQuery;
import org.example.spark.StageResult;

import java.nio.file.Path;
import java.util.List;

/**
 * Полный прогон на Spark: ./gradlew run --args="data.csv out"
 *
 * out/raw.parquet          сырой слой, один в один с CSV
 * out/rejected_ingest.csv  строки, которые не разобрались
 * out/prepared.parquet     прошедшие правила качества строки с TotalPrice, типом и флагом возврата
 * out/rejected.csv         отбракованные правилами строки с причиной
 * out/dq_report.csv        сколько строк нарушило каждое правило
 * out/top_products.csv     топ-5 товаров
 * out/customer_metrics.csv заказы, сумма и средний чек по клиентам
 * out/monthly_revenue.csv  выручка и доля возвратов по месяцам
 */
public final class Main {

    private static final int TOP_LIMIT = 5;

    public static void main(String[] args) {
        Path input = Path.of(args.length > 0 ? args[0] : "data.csv");
        Path outDir = Path.of(args.length > 1 ? args[1] : "out");
        String master = args.length > 2 ? args[2] : "local[*]";

        SparkSession spark = SparkSession.builder()
                .appName("Online Retail pipeline")
                .master(master)
                .config("spark.sql.shuffle.partitions", String.valueOf(
                        Math.max(8, Runtime.getRuntime().availableProcessors() * 2)))
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.ui.enabled", false)
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            System.out.println("== Шаг 1: загрузка CSV в parquet ==");
            StageResult ingest = pipelineStage(() -> new SparkPipeline(spark, input, outDir).ingest());
            print(ingest);

            System.out.println();
            System.out.println("== Шаг 2: правила качества ==");
            SparkPipeline pipeline = new SparkPipeline(spark, input, outDir);
            StageResult prepared = pipeline.prepare();
            print(prepared);

            System.out.println();
            System.out.printf("== Шаги 3 и 4: запросы и выгрузка (%d строк, %d партиций) ==%n",
                    pipeline.preparedRowCount(), pipeline.preparedFiles());

            List<SparkQuery> queries = List.of(
                    new org.example.spark.TopProductsQuery(TOP_LIMIT),
                    new CustomerMetricsQuery(),
                    new MonthlyRevenueQuery());

            for (SparkQuery query : queries) {
                long startedAt = System.currentTimeMillis();
                Path saved = pipeline.runQuery(query);
                System.out.printf("%s: %d мс -> %s%n",
                        query.name(), System.currentTimeMillis() - startedAt, saved);
            }
        } finally {
            spark.stop();
        }
    }

    private static StageResult pipelineStage(java.util.function.Supplier<StageResult> stage) {
        return stage.get();
    }

    private static void print(StageResult result) {
        System.out.printf("прочитано %d, принято %d, отбраковано %d, баланс %s, %d мс%n",
                result.read(), result.accepted(), result.rejected(),
                result.isBalanced() ? "ок" : "РАСХОЖДЕНИЕ", result.millis());
        result.violations().entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .forEach(entry -> System.out.printf("  %-45s %d%n", entry.getKey(), entry.getValue()));
    }
}
