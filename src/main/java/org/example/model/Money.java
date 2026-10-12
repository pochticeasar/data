package org.example.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Деньги храним и суммируем целым числом в десятитысячных долях.
 * В UnitPrice максимум 3 знака после запятой, поэтому масштаб 4 покрывает данные
 * без потерь, а суммирование long не копит ошибку, как double.
 */
public final class Money {

    public static final int SCALE = 4;

    private Money() {
    }

    public static long toScaled(BigDecimal value) {
        return value.movePointRight(SCALE).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    public static BigDecimal fromScaled(long scaled) {
        return BigDecimal.valueOf(scaled, SCALE);
    }

    /** Денежная величина для выгрузки: два знака после запятой. */
    public static String format(long scaled) {
        return fromScaled(scaled).setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    /** Среднее: делим до двух знаков, пустое при нулевом делителе. */
    public static String formatAverage(long scaledSum, long count) {
        if (count == 0) {
            return "";
        }
        return fromScaled(scaledSum)
                .divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP)
                .toPlainString();
    }
}
