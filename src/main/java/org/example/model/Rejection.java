package org.example.model;

/** Отбракованная строка: где искать в источнике, какое правило сработало. */
public record Rejection(long lineNumber, String rule, String message, String payload) {
}
