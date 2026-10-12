package org.example.parse;

import org.example.model.InvoiceLine;
import org.example.model.RawRow;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Схема Online Retail:
 * InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country.
 * Дата в формате M/d/yyyy H:mm, разделитель дробной части - точка.
 */
public final class InvoiceLineParser implements RowParser<InvoiceLine> {

    public static final int EXPECTED_FIELDS = 8;

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy H:mm");

    @Override
    public ParseResult<InvoiceLine> parse(RawRow row) {
        if (row.fields().size() != EXPECTED_FIELDS) {
            return ParseResult.error("parse.fieldCount",
                    "ожидалось " + EXPECTED_FIELDS + " полей, получено " + row.fields().size());
        }

        String invoiceNo = row.field(0).trim();
        String stockCode = row.field(1).trim();
        String description = row.field(2).trim();
        String quantityRaw = row.field(3).trim();
        String dateRaw = row.field(4).trim();
        String priceRaw = row.field(5).trim();
        String customerId = row.field(6).trim();
        String country = row.field(7).trim();

        int quantity;
        try {
            quantity = Integer.parseInt(quantityRaw);
        } catch (NumberFormatException e) {
            return ParseResult.error("parse.quantity", "не число: '" + quantityRaw + "'");
        }

        BigDecimal unitPrice;
        try {
            unitPrice = new BigDecimal(priceRaw);
        } catch (NumberFormatException e) {
            return ParseResult.error("parse.unitPrice", "не число: '" + priceRaw + "'");
        }

        LocalDateTime invoiceDate;
        try {
            invoiceDate = LocalDateTime.parse(dateRaw, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            return ParseResult.error("parse.invoiceDate", "не дата: '" + dateRaw + "'");
        }

        return ParseResult.ok(new InvoiceLine(
                row.lineNumber(),
                invoiceNo,
                stockCode,
                description,
                quantity,
                invoiceDate,
                unitPrice,
                customerId.isEmpty() ? null : customerId,
                country
        ));
    }
}
