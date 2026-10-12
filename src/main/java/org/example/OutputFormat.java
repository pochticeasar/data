package org.example;

import org.example.model.PreparedLine;
import org.example.model.Rejection;

import java.time.format.DateTimeFormatter;
import java.util.List;

/** Форматы выгрузок. Даты в ISO, суммы с двумя знаками, разделитель - запятая. */
final class OutputFormat {

    static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static final List<String> PREPARED_HEADER = List.of(
            "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
            "UnitPrice", "TotalPrice", "CustomerID", "Country", "LineKind", "LineFlow");

    static final List<String> REJECTED_HEADER = List.of(
            "LineNumber", "Rule", "Message", "RawLine");

    static final List<String> REPORT_HEADER = List.of(
            "Rule", "Severity", "Rows");

    private OutputFormat() {
    }

    static List<String> prepared(PreparedLine line) {
        return List.of(
                line.invoiceNo(),
                line.stockCode(),
                line.description(),
                String.valueOf(line.quantity()),
                line.invoiceDate().format(DATE),
                line.unitPrice().toPlainString(),
                line.totalPrice().toPlainString(),
                line.customerId() == null ? "" : line.customerId(),
                line.country(),
                line.kind().name(),
                line.flow().name());
    }

    static List<String> rejected(Rejection rejection) {
        return List.of(
                String.valueOf(rejection.lineNumber()),
                rejection.rule(),
                rejection.message(),
                rejection.rawLine());
    }
}
