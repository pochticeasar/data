package org.example.prepare;

import org.example.io.RowSource;
import org.example.model.InvoiceLine;
import org.example.parse.ParseResult;
import org.example.parse.RowParser;

import java.util.HashMap;
import java.util.Map;

/**
 * У 647 кодов из 4070 несколько разных Description. Берём самое частое
 * непустое описание, при равенстве - лексикографически меньшее, чтобы прогон
 * был воспроизводимым. Строится отдельным проходом по файлу.
 */
public final class MostFrequentDescriptionCatalog implements ProductCatalog {

    private final Map<String, String> canonical;

    private MostFrequentDescriptionCatalog(Map<String, String> canonical) {
        this.canonical = canonical;
    }

    public static ProductCatalog scan(RowSource source, RowParser<InvoiceLine> parser) {
        Map<String, Map<String, Long>> counts = new HashMap<>();
        source.rows().forEach(row -> {
            if (parser.parse(row) instanceof ParseResult.Ok<InvoiceLine>(InvoiceLine line)) {
                if (line.description().isEmpty() || line.stockCode().isEmpty()) {
                    return;
                }
                counts.computeIfAbsent(line.stockCode(), unused -> new HashMap<>())
                        .merge(line.description(), 1L, Long::sum);
            }
        });

        Map<String, String> canonical = new HashMap<>(counts.size() * 2);
        counts.forEach((code, descriptions) -> descriptions.entrySet().stream()
                .max(Map.Entry.<String, Long>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey(java.util.Comparator.reverseOrder())))
                .ifPresent(best -> canonical.put(code, best.getKey())));
        return new MostFrequentDescriptionCatalog(canonical);
    }

    @Override
    public String descriptionFor(String stockCode, String fallback) {
        return canonical.getOrDefault(stockCode, fallback);
    }

    @Override
    public int size() {
        return canonical.size();
    }
}
