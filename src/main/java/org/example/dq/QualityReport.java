package org.example.dq;

import java.util.Map;

/** Копилка статистики прогона. */
public interface QualityReport {

    void onRead();

    void onAccepted();

    void onRejected(String rule);

    void onViolation(String rule, Severity severity);

    long read();

    long accepted();

    long rejected();

    /** Правило -> сколько строк его нарушило. */
    Map<String, Long> violations();

    /** Правило -> серьёзность, для вывода в отчёт. */
    Map<String, Severity> severities();
}
