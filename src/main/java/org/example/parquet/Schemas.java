package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Схемы parquet-слоёв. Даты - epoch millis UTC, деньги - long в масштабе Money.SCALE.
 * Логические типы avro сознательно не используются: так слой читается любым
 * инструментом без согласования версий.
 */
public final class Schemas {

    public static final Schema RAW = SchemaBuilder.record("RawInvoiceLine")
            .namespace("org.example.raw").fields()
            .requiredLong("lineNumber")
            .requiredString("invoiceNo")
            .requiredString("stockCode")
            .requiredString("description")
            .requiredInt("quantity")
            .requiredLong("invoiceDateMillis")
            .requiredLong("unitPriceScaled")
            .name("customerId").type().nullable().stringType().noDefault()
            .requiredString("country")
            .endRecord();

    public static final Schema PREPARED = SchemaBuilder.record("PreparedLine")
            .namespace("org.example.prepared").fields()
            .requiredString("invoiceNo")
            .requiredString("stockCode")
            .requiredString("description")
            .requiredInt("quantity")
            .requiredLong("invoiceDateMillis")
            .requiredLong("unitPriceScaled")
            .requiredLong("totalPriceScaled")
            .name("customerId").type().nullable().stringType().noDefault()
            .requiredString("country")
            .requiredString("lineKind")
            .requiredString("lineFlow")
            .endRecord();

    private Schemas() {
    }
}
