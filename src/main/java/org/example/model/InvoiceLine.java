package org.example.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Разобранная и типизированная строка счёта до применения правил качества.
 * customerId может быть null: в датасете ~25% строк без клиента.
 */
public record InvoiceLine(
        long lineNumber,
        String invoiceNo,
        String stockCode,
        String description,
        int quantity,
        LocalDateTime invoiceDate,
        BigDecimal unitPrice,
        String customerId,
        String country
) {

    /** Счёт-отмена: номер начинается с C. */
    public boolean isCancellation() {
        return !invoiceNo.isEmpty() && Character.toUpperCase(invoiceNo.charAt(0)) == 'C';
    }

    public boolean hasCustomer() {
        return customerId != null && !customerId.isEmpty();
    }

    public BigDecimal totalPrice() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
