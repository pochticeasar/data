package org.example.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Готовая к агрегации строка: описание нормализовано по каталогу,
 * посчитан TotalPrice, проставлены тип позиции и направление (продажа/возврат).
 */
public record PreparedLine(
        String invoiceNo,
        String stockCode,
        String description,
        int quantity,
        LocalDateTime invoiceDate,
        BigDecimal unitPrice,
        BigDecimal totalPrice,
        String customerId,
        String country,
        LineKind kind,
        LineFlow flow
) {
}
