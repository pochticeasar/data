package org.example.dq;

/** Срабатывание правила на конкретной строке. */
public record Violation(String rule, Severity severity, String message) {
}
