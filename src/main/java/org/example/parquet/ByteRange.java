package org.example.parquet;

/** Полуинтервал байт в файле: [start, end). */
public record ByteRange(long start, long end) {
}
