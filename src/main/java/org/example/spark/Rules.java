package org.example.spark;

import org.apache.spark.sql.Column;
import org.example.dq.Severity;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

/** Правила качества для Online Retail. Порядок ERROR-правил задаёт причину отбраковки. */
public final class Rules {

    private Rules() {
    }

    /** Отбраковывающие правила, в порядке приоритета причины. */
    public static List<SparkRule> errors() {
        return List.of(
                new SparkRule("invoice.required", Severity.ERROR, empty("invoiceNo")),
                new SparkRule("stock.required", Severity.ERROR, empty("stockCode")),
                new SparkRule("country.required", Severity.ERROR, empty("country")),
                new SparkRule("quantity.nonzero", Severity.ERROR, col("quantity").equalTo(0)),
                new SparkRule("price.negative", Severity.ERROR, col("unitPrice").lt(0)),
                new SparkRule("date.outOfPeriod", Severity.ERROR,
                        to_date(col("invoiceDate")).lt(lit(RetailSchema.PERIOD_START.toString()))
                                .or(to_date(col("invoiceDate")).gt(lit(RetailSchema.PERIOD_END.toString())))),
                new SparkRule("row.duplicate", Severity.ERROR, col("rowSeq").gt(1)),
                // Противоречие: на счёте-отмене не может быть положительного количества.
                new SparkRule("returns.invalid", Severity.ERROR,
                        col("isCancellation").and(col("quantity").gt(0))));
    }

    /** Правила-предупреждения: строка остаётся, нарушение попадает в отчёт. */
    public static List<SparkRule> warnings() {
        return List.of(
                new SparkRule("customer.missing", Severity.WARNING, col("customerId").isNull()),
                new SparkRule("price.zero", Severity.WARNING, col("unitPrice").equalTo(0)),
                new SparkRule("description.missing", Severity.WARNING, empty("description")),
                new SparkRule("returns.negativeWithoutCancellation", Severity.WARNING,
                        col("quantity").lt(0).and(col("isCancellation").equalTo(false))),
                new SparkRule("country.unspecified", Severity.WARNING,
                        col("country").equalTo("Unspecified")));
    }

    private static Column empty(String column) {
        return col(column).isNull().or(length(col(column)).equalTo(0));
    }
}
