package org.example.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.example.model.LineFlow;
import org.example.model.LineKind;
import org.example.model.Money;
import org.example.model.PreparedLine;

import java.time.ZoneOffset;

/** PreparedLine в основном слое parquet. */
public final class PreparedLineMapper implements ParquetMapper<PreparedLine> {

    @Override
    public Schema schema() {
        return Schemas.PREPARED;
    }

    @Override
    public GenericRecord toRecord(PreparedLine line) {
        GenericRecord record = new GenericData.Record(Schemas.PREPARED);
        record.put("invoiceNo", line.invoiceNo());
        record.put("stockCode", line.stockCode());
        record.put("description", line.description());
        record.put("quantity", line.quantity());
        record.put("invoiceDateMillis", line.invoiceDate().toInstant(ZoneOffset.UTC).toEpochMilli());
        record.put("unitPriceScaled", Money.toScaled(line.unitPrice()));
        record.put("totalPriceScaled", Money.toScaled(line.totalPrice()));
        record.put("customerId", line.customerId());
        record.put("country", line.country());
        record.put("lineKind", line.kind().name());
        record.put("lineFlow", line.flow().name());
        return record;
    }

    @Override
    public PreparedLine fromRecord(GenericRecord record) {
        Object customerId = record.get("customerId");
        return new PreparedLine(
                record.get("invoiceNo").toString(),
                record.get("stockCode").toString(),
                record.get("description").toString(),
                (Integer) record.get("quantity"),
                Timestamps.toLocalDateTime((Long) record.get("invoiceDateMillis")),
                Money.fromScaled((Long) record.get("unitPriceScaled")),
                Money.fromScaled((Long) record.get("totalPriceScaled")),
                customerId == null ? null : customerId.toString(),
                record.get("country").toString(),
                LineKind.valueOf(record.get("lineKind").toString()),
                LineFlow.valueOf(record.get("lineFlow").toString()));
    }
}
