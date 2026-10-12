package org.example.model;

/** Отбракованная строка: куда смотреть в исходнике, какое правило сработало. */
public record Rejection(long lineNumber, String rule, String message, String rawLine) {
}
