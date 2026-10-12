package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.spark.SparkPipeline;
import org.example.spark.StageResult;
import org.example.support.SparkSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Шаг 2: правила качества, соответствие схеме, разметка возвратов. */
class DataQualityStageTest {

    @Test
    void rejectsRowByFirstMatchingErrorRule(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                // проходит
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                // нулевое количество
                "536366,22197,POPCORN HOLDER,0,12/1/2010 8:27,2.55,17850,United Kingdom",
                // отрицательная цена
                "536367,22197,POPCORN HOLDER,1,12/1/2010 8:28,-2.55,17850,United Kingdom",
                // положительное количество на счёте-отмене
                "C536368,22197,POPCORN HOLDER,2,12/1/2010 8:29,2.55,17850,United Kingdom",
                // дата вне периода датасета
                "536369,22197,POPCORN HOLDER,1,11/30/2010 23:59,2.55,17850,United Kingdom",
                // пустой StockCode
                "536370,,POPCORN HOLDER,1,12/1/2010 8:31,2.55,17850,United Kingdom",
                // пустая страна
                "536371,22197,POPCORN HOLDER,1,12/1/2010 8:32,2.55,17850,",
                // полный дубль первой строки
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom");

        Map<String, String> rejected = rejectionsByInvoice(pipeline);
        assertEquals("quantity.nonzero", rejected.get("536366"));
        assertEquals("price.negative", rejected.get("536367"));
        assertEquals("returns.invalid", rejected.get("C536368"));
        assertEquals("date.outOfPeriod", rejected.get("536369"));
        assertEquals("stock.required", rejected.get("536370"));
        assertEquals("country.required", rejected.get("536371"));
        assertEquals(7, rejected.size(), "дубль первой строки тоже отбракован");
        assertEquals(1, pipeline.preparedRowCount());
    }

    @Test
    void countsWarningsWithoutDroppingRows(@TempDir Path dir) {
        Path out = dir.resolve("out");
        SparkPipeline pipeline = new SparkPipeline(SparkSupport.spark(), SparkSupport.fixture(dir,
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,0,17850,United Kingdom",
                "536366,22197,,1,12/1/2010 8:27,2.55,,United Kingdom",
                "536367,22197,POPCORN HOLDER,-3,12/1/2010 8:28,2.55,17850,Unspecified"), out);
        pipeline.ingest();
        StageResult result = pipeline.prepare();

        assertEquals(3, result.accepted(), "предупреждения строку не отбраковывают");
        assertEquals(0, result.rejected());
        assertEquals(1L, result.violations().get("price.zero"));
        assertEquals(1L, result.violations().get("customer.missing"));
        assertEquals(1L, result.violations().get("country.unspecified"));
        assertEquals(1L, result.violations().get("returns.negativeWithoutCancellation"));
        // Описание подставилось из справочника, поэтому пропуска уже нет.
        assertNull(result.violations().get("description.missing"));
    }

    @Test
    void reportsDeviationsFromDeclaredSchema(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                // InvoiceNo по схеме: 6 цифр
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                // префикс A вместо 6 цифр
                "A563185,22197,POPCORN HOLDER,1,12/1/2010 8:27,2.55,17850,United Kingdom",
                // буквенный суффикс варианта товара
                "536366,85123A,WHITE HANGING HEART T-LIGHT HOLDER,1,12/1/2010 8:28,2.75,17850,United Kingdom",
                // вообще не код товара
                "536367,POST,POSTAGE,1,12/1/2010 8:29,18.00,17850,France",
                // CustomerID не 5 цифр, но число
                "536368,22197,POPCORN HOLDER,1,12/1/2010 8:30,2.55,123,United Kingdom",
                // CustomerID не число
                "536369,22197,POPCORN HOLDER,1,12/1/2010 8:31,2.55,not-a-number,United Kingdom",
                // страны нет в справочнике источника
                "536370,22197,POPCORN HOLDER,1,12/1/2010 8:32,2.55,17850,Wakanda");

        StageResult result = pipeline.prepare();
        assertEquals(1L, result.violations().get("schema.invoiceNo.format"));
        assertEquals(1L, result.violations().get("schema.stockCode.variantSuffix"));
        assertEquals(1L, result.violations().get("schema.stockCode.otherFormat"));
        assertEquals(2L, result.violations().get("schema.customerId.format"), "123 и not-a-number");
        assertEquals(1L, result.violations().get("schema.customerId.notNumeric"));
        assertEquals(1L, result.violations().get("schema.country.unknown"));

        Dataset<Row> prepared = prepared(pipeline);
        Row broken = prepared.filter("invoiceNo = '536369'").first();
        assertNull(broken.getAs("customerId"), "нечисловой CustomerID дальше считается отсутствующим");
        assertEquals("123", prepared.filter("invoiceNo = '536368'").first().getAs("customerId"),
                "числовой короткий идентификатор остаётся: это отклонение схемы, а не мусор");
    }

    @Test
    void marksReturnsAndKeepsThem(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,22197,POPCORN HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                "C536366,22197,POPCORN HOLDER,-2,12/1/2010 8:27,2.55,17850,United Kingdom",
                "536367,22197,POPCORN HOLDER,-1,12/1/2010 8:28,2.55,17850,United Kingdom");

        Dataset<Row> prepared = prepared(pipeline);
        assertEquals(1, prepared.filter("lineFlow = 'SALE'").count());
        assertEquals(2, prepared.filter("lineFlow = 'RETURN'").count(),
                "возврат и без счёта-отмены остаётся возвратом");
        assertEquals("-5.1000", prepared.filter("invoiceNo = 'C536366'").first()
                .getAs("totalPrice").toString(), "у возврата отрицательная сумма");
    }

    @Test
    void classifiesServicePositionsSeparatelyFromProducts(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,22197,POPCORN HOLDER,1,12/1/2010 8:26,2.55,17850,United Kingdom",
                "536366,85123A,WHITE HANGING HEART T-LIGHT HOLDER,1,12/1/2010 8:27,2.75,17850,United Kingdom",
                "536367,POST,POSTAGE,1,12/1/2010 8:28,18.00,17850,France",
                "536368,BANK CHARGES,Bank Charges,1,12/1/2010 8:29,15.00,17850,United Kingdom",
                "536369,M,Manual,1,12/1/2010 8:30,1.00,17850,United Kingdom",
                "536370,gift_0001_40,Gift Voucher,1,12/1/2010 8:31,34.04,17850,United Kingdom",
                "536371,S,Samples,1,12/1/2010 8:32,1.00,17850,United Kingdom",
                // мусор вместо кода: товаром считаться не должен
                "536372,3.14,bad type StockCode,1,12/1/2010 8:33,1.25,17850,United Kingdom");

        Dataset<Row> prepared = prepared(pipeline);
        assertEquals(2, prepared.filter("lineKind = 'PRODUCT'").count(), "22197 и 85123A");
        assertEquals(1, prepared.filter("lineKind = 'SHIPPING'").count());
        assertEquals(1, prepared.filter("lineKind = 'FEE'").count());
        assertEquals(1, prepared.filter("lineKind = 'ADJUSTMENT'").count());
        assertEquals(1, prepared.filter("lineKind = 'GIFT_CARD'").count());
        assertEquals(1, prepared.filter("lineKind = 'SAMPLE'").count());
        assertEquals("OTHER", prepared.filter("stockCode = '3.14'").first().getAs("lineKind"));
    }

    @Test
    void normalizesDescriptionToMostFrequentPerStockCode(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,1,12/1/2010 8:26,2.75,17850,United Kingdom",
                "536366,85123A,WHITE HANGING HEART T-LIGHT HOLDER,2,12/1/2010 8:27,2.75,17850,United Kingdom",
                "536367,85123A,wrong description rare,3,12/1/2010 8:28,2.75,17850,United Kingdom",
                "536368,85123A,,4,12/1/2010 8:29,2.75,17850,United Kingdom");

        Dataset<Row> prepared = prepared(pipeline);
        assertEquals(1, prepared.select("description").distinct().count(),
                "у одного кода остаётся одно описание");
        assertEquals("WHITE HANGING HEART T-LIGHT HOLDER",
                prepared.first().getAs("description"));
    }

    @Test
    void findsDuplicatesAfterDescriptionIsNormalized(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom",
                // то же самое, но с редким описанием: после нормализации это дубль
                "536365,85123A,rare description,6,12/1/2010 8:26,2.55,17850,United Kingdom");

        StageResult result = pipeline.prepare();
        assertEquals(2L, result.violations().get("row.duplicate"));
        assertEquals(1, pipeline.preparedRowCount());
        assertTrue(result.isBalanced());
    }

    private static Dataset<Row> prepared(SparkPipeline pipeline) {
        return SparkSupport.spark().read().parquet(pipeline.preparedParquet().toString());
    }

    private static Map<String, String> rejectionsByInvoice(SparkPipeline pipeline) {
        Dataset<Row> rejected = SparkSupport.spark().read().option("header", true)
                .csv(pipeline.preparedParquet().getParent().resolve("rejected.csv").toString());
        return rejected.collectAsList().stream().collect(java.util.stream.Collectors.toMap(
                row -> row.getAs("Payload").toString().split("/")[0],
                row -> row.getAs("Rule").toString(),
                (first, second) -> first));
    }
}
