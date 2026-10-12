package org.example.spark;

import org.apache.spark.sql.Column;
import org.example.dq.Dimension;
import org.example.dq.Severity;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

/** Правила качества для Online Retail. Порядок ERROR-правил задаёт причину отбраковки. */
public final class Rules {

    private Rules() {
    }

    /** Все правила: сначала отбраковывающие, потом предупреждения и схема. */
    public static List<SparkRule> all() {
        List<SparkRule> rules = new ArrayList<>(errors());
        rules.addAll(warnings());
        rules.addAll(schema());
        return rules;
    }

    /** Отбраковывающие правила, в порядке приоритета причины. */
    public static List<SparkRule> errors() {
        return List.of(
                new SparkRule("invoice.required", Dimension.COMPLETENESS, Severity.ERROR, empty("invoiceNo")),
                new SparkRule("stock.required", Dimension.COMPLETENESS, Severity.ERROR, empty("stockCode")),
                new SparkRule("country.required", Dimension.COMPLETENESS, Severity.ERROR, empty("country")),
                new SparkRule("quantity.nonzero", Dimension.VALIDITY, Severity.ERROR,
                        col("quantity").equalTo(0)),
                new SparkRule("price.negative", Dimension.VALIDITY, Severity.ERROR,
                        col("unitPrice").lt(0)),
                new SparkRule("date.outOfPeriod", Dimension.VALIDITY, Severity.ERROR,
                        to_date(col("invoiceDate")).lt(lit(RetailSchema.PERIOD_START.toString()))
                                .or(to_date(col("invoiceDate")).gt(lit(RetailSchema.PERIOD_END.toString())))),
                new SparkRule("row.duplicate", Dimension.UNIQUENESS, Severity.ERROR,
                        col("rowSeq").gt(1)),
                // Противоречие: на счёте-отмене не может быть положительного количества.
                new SparkRule("returns.invalid", Dimension.CONSISTENCY, Severity.ERROR,
                        col("isCancellation").and(col("quantity").gt(0))));
    }

    /** Правила-предупреждения: строка остаётся, нарушение попадает в отчёт. */
    public static List<SparkRule> warnings() {
        return List.of(
                // Схема датасета объявляет, что пропусков нет. Реально четверть строк без клиента.
                new SparkRule("customer.missing", Dimension.COMPLETENESS, Severity.WARNING,
                        col("customerId").isNull()),
                new SparkRule("price.zero", Dimension.VALIDITY, Severity.WARNING,
                        col("unitPrice").equalTo(0)),
                new SparkRule("description.missing", Dimension.COMPLETENESS, Severity.WARNING,
                        empty("description")),
                new SparkRule("returns.negativeWithoutCancellation", Dimension.CONSISTENCY, Severity.WARNING,
                        col("quantity").lt(0).and(col("isCancellation").equalTo(false))),
                new SparkRule("country.unspecified", Dimension.REFERENCE, Severity.WARNING,
                        col("country").equalTo("Unspecified")));
    }

    /**
     * Соответствие объявленной схеме датасета. Все правила этого измерения -
     * предупреждения: данные от схемы отклоняются массово (буквенные суффиксы
     * в StockCode, служебные коды, пустой CustomerID), и отбраковка по ним
     * выкинула бы корректные продажи.
     */
    public static List<SparkRule> schema() {
        return List.of(
                // Ожидается 6 цифр или C + 6 цифр. В данных есть ещё префикс A (bad debt).
                new SparkRule("schema.invoiceNo.format", Dimension.SCHEMA, Severity.WARNING,
                        col("invoiceNo").rlike(RetailSchema.INVOICE_PATTERN).equalTo(false)),
                // 5 цифр + буква: это вариант товара, допустимое отклонение от схемы.
                new SparkRule("schema.stockCode.variantSuffix", Dimension.SCHEMA, Severity.WARNING,
                        col("stockCode").rlike(RetailSchema.STOCK_VARIANT_PATTERN)),
                // Всё остальное: служебные позиции (POST, M, BANK CHARGES, gift_0001_*)
                // и коды другой длины. Товаром такая позиция быть всё ещё может,
                // классификацию делает Classifier, это правило только про схему.
                new SparkRule("schema.stockCode.otherFormat", Dimension.SCHEMA, Severity.WARNING,
                        col("stockCode").rlike(RetailSchema.STOCK_PATTERN).equalTo(false)
                                .and(col("stockCode").rlike(RetailSchema.STOCK_VARIANT_PATTERN).equalTo(false))),
                // Ожидается 5 цифр. Пустой CustomerID считает отдельное правило completeness.
                new SparkRule("schema.customerId.format", Dimension.SCHEMA, Severity.WARNING,
                        col("customerIdSource").isNotNull()
                                .and(col("customerIdSource").rlike(RetailSchema.CUSTOMER_PATTERN).equalTo(false))),
                // Нечисловой идентификатор клиента дальше считается отсутствующим:
                // приписать такую строку клиенту нельзя, а выкидывать саму продажу не за что.
                new SparkRule("schema.customerId.notNumeric", Dimension.SCHEMA, Severity.WARNING,
                        col("customerIdSource").isNotNull()
                                .and(col("customerIdSource").rlike(RetailSchema.CUSTOMER_NUMERIC_PATTERN).equalTo(false))),
                new SparkRule("schema.country.unknown", Dimension.REFERENCE, Severity.WARNING,
                        col("country").isin(RetailSchema.COUNTRIES.toArray()).equalTo(false)));
    }

    private static Column empty(String column) {
        return col(column).isNull().or(length(col(column)).equalTo(0));
    }
}
