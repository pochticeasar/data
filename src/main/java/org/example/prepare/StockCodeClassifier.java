package org.example.prepare;

import org.example.model.InvoiceLine;
import org.example.model.LineKind;

import java.util.Map;

/**
 * В StockCode зашиты не только товары: POST и DOT - доставка, M и D - ручные
 * корректировки и скидки, BANK CHARGES и AMAZONFEE - сборы, gift_0001_* - подарочные
 * сертификаты, S - образцы. Такие позиции нельзя смешивать с товарами в топ-5.
 */
public final class StockCodeClassifier implements LineClassifier {

    private static final Map<String, LineKind> SPECIAL = Map.ofEntries(
            Map.entry("POST", LineKind.SHIPPING),
            Map.entry("DOT", LineKind.SHIPPING),
            Map.entry("C2", LineKind.SHIPPING),
            Map.entry("M", LineKind.ADJUSTMENT),
            Map.entry("D", LineKind.ADJUSTMENT),
            Map.entry("B", LineKind.ADJUSTMENT),
            Map.entry("CRUK", LineKind.ADJUSTMENT),
            Map.entry("PADS", LineKind.ADJUSTMENT),
            Map.entry("S", LineKind.SAMPLE),
            Map.entry("BANK CHARGES", LineKind.FEE),
            Map.entry("AMAZONFEE", LineKind.FEE)
    );

    @Override
    public LineKind classify(InvoiceLine line) {
        String code = line.stockCode().toUpperCase();
        LineKind special = SPECIAL.get(code);
        if (special != null) {
            return special;
        }
        if (code.startsWith("GIFT_")) {
            return LineKind.GIFT_CARD;
        }
        if (code.isEmpty()) {
            return LineKind.OTHER;
        }
        return Character.isDigit(code.charAt(0)) ? LineKind.PRODUCT : LineKind.OTHER;
    }
}
