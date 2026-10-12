package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.example.model.InvoiceLine;
import org.example.model.Money;

import java.time.ZoneOffset;

/** InvoiceLine в raw-слое parquet. */
public final class RawLineMapper implements ParquetMapper<InvoiceLine> {

    @Override
    public Schema schema() {
        return Schemas.RAW;
    }

    @Override
    public GenericRecord toRecord(InvoiceLine line) {
        GenericRecord record = new GenericData.Record(Schemas.RAW);
        record.put("lineNumber", line.lineNumber());
        record.put("invoiceNo", line.invoiceNo());
        record.put("stockCode", line.stockCode());
        record.put("description", line.description());
        record.put("quantity", line.quantity());
        record.put("invoiceDateMillis", line.invoiceDate().toInstant(ZoneOffset.UTC).toEpochMilli());
        record.put("unitPriceScaled", Money.toScaled(line.unitPrice()));
        record.put("customerId", line.customerId());
        record.put("country", line.country());
        return record;
    }

    @Override
    public InvoiceLine fromRecord(GenericRecord record) {
        Object customerId = record.get("customerId");
        return new InvoiceLine(
                (Long) record.get("lineNumber"),
                record.get("invoiceNo").toString(),
                record.get("stockCode").toString(),
                record.get("description").toString(),
                (Integer) record.get("quantity"),
                Timestamps.toLocalDateTime((Long) record.get("invoiceDateMillis")),
                Money.fromScaled((Long) record.get("unitPriceScaled")),
                customerId == null ? null : customerId.toString(),
                record.get("country").toString());
    }
}
