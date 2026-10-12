package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.model.LineFlow;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count_distinct;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * Запрос 2: по клиенту число заказов, потраченная сумма и средний чек.
 * Служебные позиции входят в деньги: клиент их реально заплатил.
 * Строки без CustomerID пропускаются, привязать их к клиенту нельзя.
 *
 * Заказ - уникальный InvoiceNo. Отмена это отдельный документ, поэтому она
 * попадает в OrdersTotal и не попадает в OrdersSales.
 */
public final class CustomerMetricsQuery implements SparkQuery {

    @Override
    public String name() {
        return "customer_metrics";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> prepared) {
        Dataset<Row> aggregated = prepared
                .filter(col("customerId").isNotNull())
                .groupBy(col("customerId"))
                .agg(
                        count_distinct(when(col("lineFlow").equalTo(LineFlow.SALE.name()), col("invoiceNo")))
                                .as("OrdersSales"),
                        count_distinct(col("invoiceNo")).as("OrdersTotal"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.SALE.name()), col("totalPrice"))), lit(0))
                                .cast(RetailSchema.MONEY_TYPE).as("gross"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.RETURN.name()), col("totalPrice").multiply(-1))), lit(0))
                                .cast(RetailSchema.MONEY_TYPE).as("returns"))
                .withColumn("net", col("gross").minus(col("returns")).cast(RetailSchema.MONEY_TYPE));

        return aggregated
                .orderBy(col("net").desc(), col("customerId").asc())
                .select(
                        col("customerId").as("CustomerID"),
                        col("OrdersSales"),
                        col("OrdersTotal"),
                        col("gross").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("TotalGross"),
                        col("returns").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("ReturnsAmount"),
                        col("net").cast(RetailSchema.OUTPUT_MONEY_TYPE).as("TotalNet"),
                        average(col("gross"), col("OrdersSales")).as("AvgCheckGross"),
                        average(col("net"), col("OrdersTotal")).as("AvgCheckNet"));
    }

    private static org.apache.spark.sql.Column average(org.apache.spark.sql.Column sum, org.apache.spark.sql.Column count) {
        return when(count.equalTo(0), lit(null))
                .otherwise(sum.divide(count))
                .cast(RetailSchema.OUTPUT_MONEY_TYPE);
    }
}
