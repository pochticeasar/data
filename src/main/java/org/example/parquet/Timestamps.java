package org.example.parquet;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

final class Timestamps {

    private Timestamps() {
    }

    static LocalDateTime toLocalDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }
}
