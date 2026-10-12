package org.example.ingest;

import java.util.Map;

/** Итоги загрузки сырого слоя. */
public record IngestResult(long read, long written, long rejected, Map<String, Long> parseErrors) {

    public boolean isBalanced() {
        return read == written + rejected;
    }
}
