package org.example.model;

import java.util.List;

/** Сырая строка CSV: номер строки в файле, разобранные поля, исходный текст. */
public record RawRow(long lineNumber, List<String> fields, String rawLine) {

    public String field(int index) {
        return index < fields.size() ? fields.get(index) : "";
    }
}
