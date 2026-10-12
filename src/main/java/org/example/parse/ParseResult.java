package org.example.parse;

/** Результат парсинга: либо значение, либо причина отказа. */
public sealed interface ParseResult<T> {

    record Ok<T>(T value) implements ParseResult<T> {
    }

    record Error<T>(String rule, String message) implements ParseResult<T> {
    }

    static <T> ParseResult<T> ok(T value) {
        return new Ok<>(value);
    }

    static <T> ParseResult<T> error(String rule, String message) {
        return new Error<>(rule, message);
    }
}
