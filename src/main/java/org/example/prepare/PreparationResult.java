package org.example.prepare;

import org.example.dq.Severity;

import java.util.Map;

/** Итоги подготовки данных. */
public record PreparationResult(
        long read,
        long accepted,
        long rejected,
        int catalogSize,
        Map<String, Long> violations,
        Map<String, Severity> severities
) {

    /** Инвариант: ничего не потеряли по пути. */
    public boolean isBalanced() {
        return read == accepted + rejected;
    }
}
