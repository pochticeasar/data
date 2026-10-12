package org.example.dq;

import java.util.LinkedHashMap;
import java.util.Map;

/** Счётчики в памяти. Не потокобезопасно, рассчитано на один поток обработки. */
public final class InMemoryQualityReport implements QualityReport {

    private final Map<String, Long> violations = new LinkedHashMap<>();
    private final Map<String, Severity> severities = new LinkedHashMap<>();
    private long read;
    private long accepted;
    private long rejected;

    @Override
    public void onRead() {
        read++;
    }

    @Override
    public void onAccepted() {
        accepted++;
    }

    @Override
    public void onRejected(String rule) {
        rejected++;
    }

    @Override
    public void onViolation(String rule, Severity severity) {
        violations.merge(rule, 1L, Long::sum);
        severities.putIfAbsent(rule, severity);
    }

    @Override
    public long read() {
        return read;
    }

    @Override
    public long accepted() {
        return accepted;
    }

    @Override
    public long rejected() {
        return rejected;
    }

    @Override
    public Map<String, Long> violations() {
        return violations;
    }

    @Override
    public Map<String, Severity> severities() {
        return severities;
    }
}
