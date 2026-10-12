package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.example.model.LineFlow;
import org.example.model.LineKind;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

/**
 * Запрос 1: топ товаров по количеству проданных единиц. Служебные позиции
 * исключены, возвраты показаны отдельной колонкой, сортировка по количеству
 * за вычетом возвратов.
 */
public final class TopProductsQuery implements SparkQuery {

    private final int limit;

    public TopProductsQuery(int limit) {
        this.limit = limit;
    }

    @Override
    public String name() {
        return "top_products";
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> prepared) {
        return prepared
                .filter(col("lineKind").equalTo(LineKind.PRODUCT.name()))
                .groupBy(col("stockCode"))
                .agg(
                        first(col("description")).as("Description"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.SALE.name()), col("quantity"))), lit(0))
                                .as("QuantitySold"),
                        coalesce(sum(when(col("lineFlow").equalTo(LineFlow.RETURN.name()), col("quantity").multiply(-1))), lit(0))
                                .as("QuantityReturned"),
                        coalesce(sum(col("totalPrice")), lit(0)).cast(RetailSchema.OUTPUT_MONEY_TYPE)
                                .as("RevenueNet"))
                .withColumn("QuantityNet", col("QuantitySold").minus(col("QuantityReturned")))
                .orderBy(col("QuantityNet").desc(), col("stockCode").asc())
                .limit(limit)
                // Номер в рейтинге нумеруется после отсечения топа: окно без
                // partitionBy на пяти строках дешевле, чем на всём наборе.
                .withColumn("Rank", row_number().over(
                        Window.orderBy(col("QuantityNet").desc(), col("stockCode").asc())))
                .select(
                        col("Rank"),
                        col("stockCode").as("StockCode"),
                        col("Description"),
                        col("QuantitySold"),
                        col("QuantityReturned"),
                        col("QuantityNet"),
                        col("RevenueNet"));
    }
}
