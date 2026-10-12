package org.example.io;

import java.util.ArrayList;
import java.util.List;

/** Разбор одной строки CSV с учётом кавычек и экранированных кавычек. */
public final class CsvLineSplitter {

    private final char delimiter;

    public CsvLineSplitter(char delimiter) {
        this.delimiter = delimiter;
    }

    public List<String> split(String line) {
        List<String> out = new ArrayList<>(8);
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    current.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == delimiter) {
                out.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        out.add(current.toString());
        return out;
    }
}
