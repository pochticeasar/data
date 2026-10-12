package org.example.spark;

import org.apache.spark.sql.Column;
import org.example.model.LineFlow;
import org.example.model.LineKind;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

/**
 * В StockCode зашиты не только товары: POST и DOT - доставка, M, D, B - ручные
 * корректировки, BANK CHARGES и AMAZONFEE - сборы, gift_0001_* - сертификаты,
 * S - образцы. В топ товаров такие позиции попадать не должны.
 */
public final class Classifier {

    private Classifier() {
    }

    public static Column lineKind() {
        Column code = upper(col("stockCode"));
        return when(code.isin("POST", "DOT", "C2"), lit(LineKind.SHIPPING.name()))
                .when(code.isin("M", "D", "B", "CRUK", "PADS"), lit(LineKind.ADJUSTMENT.name()))
                .when(code.equalTo("S"), lit(LineKind.SAMPLE.name()))
                .when(code.isin("BANK CHARGES", "AMAZONFEE"), lit(LineKind.FEE.name()))
                .when(code.startsWith("GIFT_"), lit(LineKind.GIFT_CARD.name()))
                .when(substring(code, 1, 1).rlike("^[0-9]$"), lit(LineKind.PRODUCT.name()))
                .otherwise(lit(LineKind.OTHER.name()));
    }

    /**
     * Отрицательное количество это возврат: и на счёте-отмене, и на обычном счёте
     * (там это списание или пересчёт склада, отдельное предупреждение).
     * Возвраты не удаляются, решение о неттинге принимается на уровне запроса.
     */
    public static Column lineFlow() {
        return when(col("quantity").lt(0), lit(LineFlow.RETURN.name()))
                .otherwise(lit(LineFlow.SALE.name()));
    }
}
