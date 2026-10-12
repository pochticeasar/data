package org.example;

import org.apache.spark.sql.Row;
import org.example.spark.CustomerMetricsQuery;
import org.example.spark.MonthlyRevenueQuery;
import org.example.spark.SparkPipeline;
import org.example.spark.SparkQuery;
import org.example.spark.TopProductsQuery;
import org.example.support.SparkSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Шаги 3 и 4: три запроса и их выгрузки. */
class QueriesTest {

    @Test
    void ranksProductsByQuantityNetAndSkipsServicePositions(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                // 22197: продано 30, возвращено 5, netto 25
                "536365,22197,POPCORN HOLDER,30,12/1/2010 8:26,2.00,17850,United Kingdom",
                "C536366,22197,POPCORN HOLDER,-5,12/1/2010 8:27,2.00,17850,United Kingdom",
                // 85123A: netto 40, должен быть первым
                "536367,85123A,WHITE HANGING HEART T-LIGHT HOLDER,40,12/1/2010 8:28,3.00,17850,United Kingdom",
                // доставка дороже и количеством больше, но это не товар
                "536368,POST,POSTAGE,100,12/1/2010 8:29,18.00,17851,France",
                // вернули больше, чем купили: в рейтинге популярности такому не место
                "536369,21777,MIRROR,2,12/1/2010 8:30,1.00,17851,United Kingdom",
                "C536370,21777,MIRROR,-10,12/1/2010 8:31,1.00,17851,United Kingdom");

        List<Row> top = run(pipeline, new TopProductsQuery(5));
        assertEquals(2, top.size(), "в топе только товары с положительным netto");

        assertEquals("1", top.get(0).getAs("Rank"));
        assertEquals("85123A", top.get(0).getAs("StockCode"));
        assertEquals("40", top.get(0).getAs("QuantityNet"));
        assertEquals("120.00", top.get(0).getAs("RevenueNet"));

        assertEquals("2", top.get(1).getAs("Rank"));
        assertEquals("22197", top.get(1).getAs("StockCode"));
        assertEquals("30", top.get(1).getAs("QuantitySold"));
        assertEquals("5", top.get(1).getAs("QuantityReturned"));
        assertEquals("25", top.get(1).getAs("QuantityNet"));

        List<String> codes = top.stream().map(row -> row.getAs("StockCode").toString()).toList();
        assertFalse(codes.contains("POST"), "доставка не товар");
        assertFalse(codes.contains("21777"), "товар с отрицательным netto в топ не идёт");
    }

    @Test
    void limitsTopToRequestedSize(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,10001,A,5,12/1/2010 8:26,1.00,17850,United Kingdom",
                "536366,10002,B,4,12/1/2010 8:27,1.00,17850,United Kingdom",
                "536367,10003,C,3,12/1/2010 8:28,1.00,17850,United Kingdom");

        assertEquals(2, run(pipeline, new TopProductsQuery(2)).size());
    }

    @Test
    void countsOrdersMoneyAndAverageCheckPerCustomer(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                // клиент 17850: два заказа-продажи на 100 и 50, возврат на 20
                "536365,22197,POPCORN HOLDER,10,12/1/2010 8:26,10.00,17850,United Kingdom",
                "536366,22197,POPCORN HOLDER,5,12/2/2010 8:26,10.00,17850,United Kingdom",
                "C536367,22197,POPCORN HOLDER,-2,12/3/2010 8:26,10.00,17850,United Kingdom",
                // доставка входит в деньги клиента
                "536366,POST,POSTAGE,1,12/2/2010 8:26,25.00,17850,United Kingdom",
                // строка без клиента в метрики не попадает
                "536368,22197,POPCORN HOLDER,1,12/4/2010 8:26,10.00,,United Kingdom");

        Map<String, Row> byCustomer = run(pipeline, new CustomerMetricsQuery()).stream()
                .collect(Collectors.toMap(row -> row.getAs("CustomerID").toString(), row -> row));
        assertEquals(1, byCustomer.size(), "клиент только один, строка без CustomerID пропущена");

        Row customer = byCustomer.get("17850");
        assertEquals("2", customer.getAs("OrdersSales"));
        assertEquals("3", customer.getAs("OrdersTotal"), "отмена это отдельный документ");
        assertEquals("175.00", customer.getAs("TotalGross"), "100 + 50 + 25 за доставку");
        assertEquals("20.00", customer.getAs("ReturnsAmount"));
        assertEquals("155.00", customer.getAs("TotalNet"));
        assertEquals("87.50", customer.getAs("AvgCheckGross"), "175 / 2 заказа-продажи");
        assertEquals("51.67", customer.getAs("AvgCheckNet"), "155 / 3 документа");
    }

    @Test
    void writesZeroAverageForCustomerWithoutSales(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,22197,POPCORN HOLDER,10,12/1/2010 8:26,2.55,17850,United Kingdom",
                "C536379,22197,POPCORN HOLDER,-2,12/7/2010 9:00,2.55,19999,United Kingdom");

        Row onlyReturns = run(pipeline, new CustomerMetricsQuery()).stream()
                .filter(row -> "19999".equals(row.getAs("CustomerID")))
                .findFirst().orElseThrow();

        assertEquals("0", onlyReturns.getAs("OrdersSales"));
        assertEquals("0.00", onlyReturns.getAs("TotalGross"));
        assertEquals("-5.10", onlyReturns.getAs("TotalNet"));
        assertEquals("0.00", onlyReturns.getAs("AvgCheckGross"),
                "средний чек без заказов-продаж это 0.00, а не пустая ячейка");
    }

    @Test
    void aggregatesRevenueByMonthWithReturnShare(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir,
                "536365,22197,POPCORN HOLDER,10,12/1/2010 8:26,10.00,17850,United Kingdom",
                "C536366,22197,POPCORN HOLDER,-1,12/2/2010 8:26,10.00,17850,United Kingdom",
                "536367,22197,POPCORN HOLDER,20,1/5/2011 8:26,10.00,17850,United Kingdom");

        List<Row> months = run(pipeline, new MonthlyRevenueQuery());
        assertEquals(2, months.size());

        Row december = months.get(0);
        assertEquals("2010-12", december.getAs("Month"));
        assertEquals("2", december.getAs("Orders"), "продажа и отмена это два документа");
        assertEquals("100.00", december.getAs("RevenueSales"));
        assertEquals("10.00", december.getAs("Returns"));
        assertEquals("90.00", december.getAs("RevenueNet"));
        assertEquals("10.00", december.getAs("ReturnsSharePct"));

        Row january = months.get(1);
        assertEquals("2011-01", january.getAs("Month"));
        assertEquals("200.00", january.getAs("RevenueNet"));
        assertEquals("0.00", january.getAs("ReturnsSharePct"));
    }

    @Test
    void writesHeaderOnlyFileForEmptyResult(@TempDir Path dir) {
        SparkPipeline pipeline = SparkSupport.prepared(dir);
        Path saved = pipeline.runQuery(new TopProductsQuery(5));

        List<String> lines = read(saved);
        assertEquals(1, lines.size(), "остаётся только шапка");
        assertTrue(lines.get(0).startsWith("Rank,StockCode"));
    }

    private static List<Row> run(SparkPipeline pipeline, SparkQuery query) {
        Path saved = pipeline.runQuery(query);
        return SparkSupport.spark().read().option("header", true)
                .csv(saved.toString())
                .collectAsList();
    }

    private static List<String> read(Path path) {
        try {
            return java.nio.file.Files.readAllLines(path);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
