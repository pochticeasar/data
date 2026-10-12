package org.example.dq;

import org.example.model.InvoiceLine;

import java.util.Optional;
import java.util.function.Function;

/** Правило без состояния, собранное из лямбды. */
public record SimpleRule(String name, Severity severity, Function<InvoiceLine, Optional<String>> body)
        implements QualityRule {

    @Override
    public Optional<String> check(InvoiceLine line) {
        return body.apply(line);
    }
}
