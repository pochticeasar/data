package org.example;

import org.example.model.Rejection;

import java.util.List;

/** Форматы текстовых выгрузок. Разделитель - запятая, кодировка UTF-8. */
final class OutputFormat {

    static final List<String> REJECTED_HEADER = List.of("LineNumber", "Rule", "Message", "Payload");

    static final List<String> REPORT_HEADER = List.of("Rule", "Severity", "Rows");

    private OutputFormat() {
    }

    static List<String> rejected(Rejection rejection) {
        return List.of(
                String.valueOf(rejection.lineNumber()),
                rejection.rule(),
                rejection.message(),
                rejection.payload());
    }
}
