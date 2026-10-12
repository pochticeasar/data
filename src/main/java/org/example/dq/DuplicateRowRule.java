package org.example.dq;

import org.example.model.InvoiceLine;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Полные дубли строк. В исходном файле их 5268.
 * Держит отпечатки всех виденных строк, поэтому требует памяти порядка размера файла.
 */
public final class DuplicateRowRule implements QualityRule {

    private final Set<String> seen = new HashSet<>(1 << 20);

    @Override
    public String name() {
        return "row.duplicate";
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Optional<String> check(InvoiceLine line) {
        String fingerprint = String.join("|",
                line.invoiceNo(),
                line.stockCode(),
                line.description(),
                String.valueOf(line.quantity()),
                line.invoiceDate().toString(),
                line.unitPrice().toPlainString(),
                String.valueOf(line.customerId()),
                line.country());
        return seen.add(fingerprint) ? Optional.empty() : Optional.of("дубль строки");
    }
}
