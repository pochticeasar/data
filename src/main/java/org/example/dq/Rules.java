package org.example.dq;

import org.example.model.InvoiceLine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/** Набор правил качества для Online Retail. */
public final class Rules {

    /** Период датасета по описанию на UCI: 01.12.2010 - 09.12.2011. */
    public static final LocalDate PERIOD_START = LocalDate.of(2010, 12, 1);
    public static final LocalDate PERIOD_END = LocalDate.of(2011, 12, 9);

    private Rules() {
    }

    public static List<QualityRule> defaults() {
        return List.of(
                required("invoice.required", InvoiceLine::invoiceNo),
                required("stock.required", InvoiceLine::stockCode),
                required("country.required", InvoiceLine::country),
                new SimpleRule("quantity.nonzero", Severity.ERROR, line ->
                        line.quantity() == 0
                                ? Optional.of("Quantity = 0")
                                : Optional.empty()),
                new SimpleRule("price.negative", Severity.ERROR, line ->
                        line.unitPrice().signum() < 0
                                ? Optional.of("UnitPrice = " + line.unitPrice().toPlainString())
                                : Optional.empty()),
                new SimpleRule("date.outOfPeriod", Severity.ERROR, line -> {
                    LocalDate date = line.invoiceDate().toLocalDate();
                    return date.isBefore(PERIOD_START) || date.isAfter(PERIOD_END)
                            ? Optional.of("дата вне периода: " + date)
                            : Optional.empty();
                }),
                new DuplicateRowRule(),
                new SimpleRule("price.zero", Severity.WARNING, line ->
                        line.unitPrice().compareTo(BigDecimal.ZERO) == 0
                                ? Optional.of("нулевая цена")
                                : Optional.empty()),
                new SimpleRule("customer.missing", Severity.WARNING, line ->
                        line.hasCustomer()
                                ? Optional.empty()
                                : Optional.of("нет CustomerID")),
                new SimpleRule("description.missing", Severity.WARNING, line ->
                        line.description().isEmpty()
                                ? Optional.of("нет Description")
                                : Optional.empty()),
                new SimpleRule("returns.negativeWithoutCancellation", Severity.WARNING, line ->
                        line.quantity() < 0 && !line.isCancellation()
                                ? Optional.of("отрицательное количество на обычном счёте")
                                : Optional.empty()),
                new SimpleRule("country.unspecified", Severity.WARNING, line ->
                        "Unspecified".equalsIgnoreCase(line.country())
                                ? Optional.of("страна не указана")
                                : Optional.empty())
        );
    }

    private static QualityRule required(String name, java.util.function.Function<InvoiceLine, String> field) {
        return new SimpleRule(name, Severity.ERROR, line -> {
            String value = field.apply(line);
            return value == null || value.isEmpty()
                    ? Optional.of("пустое обязательное поле")
                    : Optional.empty();
        });
    }
}
