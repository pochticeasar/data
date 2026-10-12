package org.example.spark;

import java.util.Map;

/** Итоги шага: сколько прочитано, принято, отбраковано и какие правила сработали. */
public record StageResult(long read, long accepted, long rejected, Map<String, Long> violations, long millis) {

    public boolean isBalanced() {
        return read == accepted + rejected;
    }
}
