package org.example.support;

import org.apache.spark.sql.SparkSession;
import org.example.spark.SparkPipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Одна сессия Spark на весь прогон тестов: поднимать её на каждый класс дорого.
 * Main останавливает сессию сам, поэтому перед выдачей проверяем, что она жива.
 */
public final class SparkSupport {

    /** Шапка датасета Online Retail. */
    public static final String HEADER =
            "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country";

    private static SparkSession session;

    private SparkSupport() {
    }

    public static synchronized SparkSession spark() {
        if (session == null || session.sparkContext().isStopped()) {
            session = SparkSession.builder()
                    .appName("tests")
                    .master("local[2]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.ui.enabled", false)
                    .getOrCreate();
            session.sparkContext().setLogLevel("ERROR");
        }
        return session;
    }

    /** Пишет CSV в кодировке источника: шапка плюс переданные строки. */
    public static Path fixture(Path dir, String... rows) {
        StringBuilder content = new StringBuilder(HEADER).append('\n');
        for (String row : rows) {
            content.append(row).append('\n');
        }
        try {
            Files.createDirectories(dir);
            Path csv = dir.resolve("fixture.csv");
            Files.writeString(csv, content.toString(), StandardCharsets.ISO_8859_1);
            return csv;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Пайплайн на фикстуре, доведённый до подготовленного слоя. */
    public static SparkPipeline prepared(Path dir, String... rows) {
        SparkPipeline pipeline = new SparkPipeline(spark(), fixture(dir, rows), dir.resolve("out"));
        pipeline.ingest();
        pipeline.prepare();
        return pipeline;
    }
}
