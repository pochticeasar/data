package org.example.dq;

import org.example.model.InvoiceLine;

import java.util.Optional;

/**
 * Правило качества. Может быть с состоянием (например, поиск дублей),
 * поэтому экземпляр правила не переиспользуется между прогонами и не потокобезопасен.
 */
public interface QualityRule {

    String name();

    Severity severity();

    /** Пустой Optional - строка правило не нарушает. */
    Optional<String> check(InvoiceLine line);
}
