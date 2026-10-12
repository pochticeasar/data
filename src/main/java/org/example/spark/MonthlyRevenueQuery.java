package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.model.LineFlow;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count_distinct;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * Запрос 3: выручка, возвраты и их доля по месяцам. Перекрёстная проверка:
 * суммы по месяцам должны складываться в общий итог, а всплеск доли возвратов
 * в месяце - сигнал для DQ.
 */
public final class MonthlyRevenueQuery implements SparkQuery {

    @Override
    public String name() {
        return "monthly_revenue";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> prepared) {
        Dataset<Row> aggregated = prepared
                .withColumn("month", date_format(col("invoiceDate"), "yyyy-MM"))
                .groupBy(col("month"))
                .agg(
                        count_distinct(col("invoiceNo")).as("Orders"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.SALE.name()), col("totalPrice"))), lit(0))
                                .cast(RetailSchema.MONEY_TYPE).as("sales"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.RETURN.name()), col("totalPrice").multiply(-1))), lit(0))
                                .cast(RetailSchema.MONEY_TYPE).as("returns"))
                .withColumn("net", col("sales").minus(col("returns")).cast(RetailSchema.MONEY_TYPE));

        return aggregated
                .orderBy(col("month").asc())
                .select(
                        col("month").as("Month"),
                        col("Orders"),
                        col("sales").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("RevenueSales"),
                        col("returns").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("Returns"),
                        col("net").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("RevenueNet"),
                        when(col("sales").equalTo(0), lit(null))
                                .otherwise(col("returns").multiply(100).divide(col("sales")))
                                .cast("decimal(10,2)").as("ReturnsSharePct"));
    }
}
