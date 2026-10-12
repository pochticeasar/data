package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.spark.SparkPipeline;
import org.example.spark.StageResult;
import org.example.spark.TopProductsQuery;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Прогон пайплайна на фикстуре, где ответы посчитаны руками.
 * Фикстура покрывает все классы отбраковки и оба направления движения товара.
 */
class SparkPipelineTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.ui.enabled", false)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void appliesRulesAndRanksProductsByNetQuantity(@TempDir Path dir) throws Exception {
        Path csv = dir.resolve("fixture.csv");
        Files.writeString(csv, String.join("\n",
                "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country",
                // продажи: 10 + 4 единицы товара 22197
                "536365,22197,POPCORN HOLDER,10,12/1/2010 8:26,2.55,17850,United Kingdom",
                "536366,22197,POPCORN HOLDER,4,12/2/2010 9:10,2.55,17850,United Kingdom",
                // возврат 3 единиц на счёте-отмене, netto по 22197 = 11
                "C536367,22197,POPCORN HOLDER,-3,12/3/2010 9:15,2.55,17850,United Kingdom",
                // товар 85123A: 7 единиц, лидером не станет
                "536368,85123A,WHITE HANGING HEART T-LIGHT HOLDER,7,12/4/2010 10:00,2.75,17851,United Kingdom",
                // списание без счёта-отмены: остаётся как возврат, но с предупреждением
                "536369,85123A,WHITE HANGING HEART T-LIGHT HOLDER,-2,12/5/2010 10:05,2.75,17851,United Kingdom",
                // доставка: в деньги входит, в топ товаров нет
                "536370,POST,POSTAGE,1,12/6/2010 11:00,18.00,17851,France",
                // без CustomerID: предупреждение, строка остаётся
                "536371,22197,POPCORN HOLDER,1,12/7/2010 11:30,2.55,,United Kingdom",
                // отбраковка: нулевое количество
                "536372,22197,POPCORN HOLDER,0,12/8/2010 12:00,2.55,17850,United Kingdom",
                // отбраковка: отрицательная цена
                "536373,22197,POPCORN HOLDER,1,12/9/2010 12:05,-2.55,17850,United Kingdom",
                // отбраковка: положительное количество на счёте-отмене
                "C536374,22197,POPCORN HOLDER,2,12/10/2010 12:10,2.55,17850,United Kingdom",
                // отбраковка: дата вне периода датасета
                "536375,22197,POPCORN HOLDER,1,1/5/2009 12:15,2.55,17850,United Kingdom",
                // отбраковка: полный дубль предыдущей строки 536368
                "536368,85123A,WHITE HANGING HEART T-LIGHT HOLDER,7,12/4/2010 10:00,2.75,17851,United Kingdom",
                // отбраковка на шаге загрузки: количество не число
                "536376,22197,POPCORN HOLDER,not-a-number,12/11/2010 12:20,2.55,17850,United Kingdom",
                ""), StandardCharsets.ISO_8859_1);

        Path out = dir.resolve("out");
        SparkPipeline pipeline = new SparkPipeline(spark, csv, out);

        StageResult ingest = pipeline.ingest();
        assertEquals(13, ingest.read());
        assertEquals(1, ingest.rejected(), "строка с нечисловым Quantity должна уйти в reject загрузки");
        assertTrue(ingest.isBalanced());

        StageResult prepared = pipeline.prepare();
        assertEquals(12, prepared.read());
        assertEquals(5, prepared.rejected(), "ноль, минусовая цена, отмена с плюсом, дата, дубль");
        assertEquals(7, prepared.accepted());
        assertTrue(prepared.isBalanced());

        assertEquals(1L, prepared.violations().get("customer.missing"));
        assertEquals(1L, prepared.violations().get("returns.negativeWithoutCancellation"));
        assertEquals(1L, prepared.violations().get("row.duplicate"));
        assertEquals(1L, prepared.violations().get("price.negative"));

        pipeline.runQuery(new TopProductsQuery(5));
        Dataset<Row> top = spark.read().option("header", true)
                .csv(out.resolve("top_products.csv").toString());
        List<Row> rows = top.collectAsList();

        assertEquals(2, rows.size(), "в топ идут только товары, доставка исключена");
        Row first = rows.get(0);
        assertEquals("22197", first.getAs("StockCode"));
        assertEquals("15", first.getAs("QuantitySold"));
        assertEquals("3", first.getAs("QuantityReturned"));
        assertEquals("12", first.getAs("QuantityNet"));
        assertEquals("85123A", rows.get(1).getAs("StockCode"));
    }
}
