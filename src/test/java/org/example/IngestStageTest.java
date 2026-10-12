package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.SparkPipeline;
import org.example.spark.StageResult;
import org.example.support.SparkSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Шаг 1: приведение типов и отбраковка технически битых строк. */
class IngestStageTest {

    @Test
    void rejectsOnlyRowsWithBrokenTypes(@TempDir Path dir) {
        Path csv = SparkSupport.fixture(dir,
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                "536366,22197,POPCORN HOLDER,abc,12/1/2010 8:27,2.55,17850,United Kingdom",
                "536367,22197,POPCORN HOLDER,1,12/1/2010 8:28,n/a,17850,United Kingdom",
                "536368,22197,POPCORN HOLDER,1,not-a-date,2.55,17850,United Kingdom",
                "536369,22197,POPCORN HOLDER,1,2010-12-01 08:26,2.55,17850,United Kingdom",
                "536370,22197,POPCORN HOLDER,1,12/1/2010 8:30,2.55,,United Kingdom");

        SparkPipeline pipeline = new SparkPipeline(SparkSupport.spark(), csv, dir.resolve("out"));
        StageResult result = pipeline.ingest();

        assertEquals(6, result.read());
        assertEquals(4, result.rejected(), "битые quantity, price и две даты");
        assertEquals(2, result.accepted());
        assertTrue(result.isBalanced());

        Map<String, Long> reasons = reasons(dir.resolve("out").resolve("rejected_ingest.csv"));
        assertEquals(1L, reasons.get("parse.quantity"));
        assertEquals(1L, reasons.get("parse.unitPrice"));
        assertEquals(2L, reasons.get("parse.invoiceDate"), "ISO-формат даты схемой не предусмотрен");
    }

    @Test
    void writesTypedColumnsToRawLayer(@TempDir Path dir) {
        Path csv = SparkSupport.fixture(dir,
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.550,17850,United Kingdom",
                "536370,22197,POPCORN HOLDER,1,12/1/2010 8:30,2.55,,United Kingdom");

        SparkPipeline pipeline = new SparkPipeline(SparkSupport.spark(), csv, dir.resolve("out"));
        pipeline.ingest();

        Dataset<Row> raw = SparkSupport.spark().read().parquet(pipeline.rawParquet().toString());
        assertEquals("int", raw.schema().apply("quantity").dataType().simpleString());
        assertEquals("timestamp", raw.schema().apply("invoiceDate").dataType().simpleString());
        assertEquals("decimal(20,4)", raw.schema().apply("unitPrice").dataType().simpleString());

        Row first = raw.orderBy("lineNumber")
                // Timestamp.toString печатает дату в таймзоне JVM, а слой пишется в UTC,
                // поэтому форматируем средствами Spark, иначе тест зависит от машины.
                .withColumn("formattedDate", date_format(col("invoiceDate"), "yyyy-MM-dd HH:mm:ss"))
                .first();
        assertEquals(6, (int) first.getAs("quantity"));
        assertEquals("2010-12-01 08:26:00", first.getAs("formattedDate"));
        assertEquals("2.5500", first.getAs("unitPrice").toString());

        Row withoutCustomer = raw.orderBy("lineNumber").collectAsList().get(1);
        assertNull(withoutCustomer.getAs("customerId"), "пустой CustomerID это null, а не пустая строка");
    }

    @Test
    void reportsMissingAndEmptyInputClearly(@TempDir Path dir) throws Exception {
        SparkPipeline missing = new SparkPipeline(
                SparkSupport.spark(), dir.resolve("no-such.csv"), dir.resolve("out"));
        assertThrows(UncheckedIOException.class, missing::ingest);

        Path empty = dir.resolve("empty.csv");
        Files.writeString(empty, "");
        SparkPipeline emptyPipeline = new SparkPipeline(SparkSupport.spark(), empty, dir.resolve("out"));
        assertThrows(IllegalArgumentException.class, emptyPipeline::ingest);
    }

    @Test
    void handlesFileWithHeaderOnly(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir);
        assertEquals(0, pipeline.preparedRowCount(), "пустой вход это не ошибка, это ноль строк");
    }

    private static Map<String, Long> reasons(Path rejected) {
        return SparkSupport.spark().read().option("header", true).csv(rejected.toString())
                .collectAsList().stream()
                .collect(Collectors.groupingBy(row -> row.getAs("Rule").toString(), Collectors.counting()));
    }
}
