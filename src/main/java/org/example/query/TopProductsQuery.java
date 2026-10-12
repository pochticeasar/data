package org.example.query;

import org.example.model.LineFlow;
import org.example.model.LineKind;
import org.example.model.Money;
import org.example.model.PreparedLine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Запрос 1: топ-N товаров по количеству проданных единиц.
 * Служебные позиции (доставка, сборы, корректировки) исключены, в рейтинг идёт
 * только LineKind.PRODUCT. Возвраты показаны отдельной колонкой, сортировка по
 * количеству за вычетом возвратов.
 */
public final class TopProductsQuery implements Query<Map<String, TopProductsQuery.Stat>> {

    private final int limit;

    public TopProductsQuery(int limit) {
        this.limit = limit;
    }

    public static final class Stat {
        String description = "";
        long quantitySold;
        long quantityReturned;
        long revenueNetScaled;

        long quantityNet() {
            return quantitySold - quantityReturned;
        }

        void merge(Stat other) {
            if (description.isEmpty()) {
                description = other.description;
            }
            quantitySold += other.quantitySold;
            quantityReturned += other.quantityReturned;
            revenueNetScaled += other.revenueNetScaled;
        }
    }

    @Override
    public String name() {
        return "top_products";
    }

    @Override
    public Map<String, Stat> newAccumulator() {
        return new HashMap<>();
    }

    @Override
    public void accumulate(Map<String, Stat> accumulator, PreparedLine line) {
        if (line.kind() != LineKind.PRODUCT) {
            return;
        }
        Stat stat = accumulator.computeIfAbsent(line.stockCode(), unused -> new Stat());
        if (stat.description.isEmpty()) {
            stat.description = line.description();
        }
        if (line.flow() == LineFlow.RETURN) {
            stat.quantityReturned += -line.quantity();
        } else {
            stat.quantitySold += line.quantity();
        }
        stat.revenueNetScaled += Money.toScaled(line.totalPrice());
    }

    @Override
    public Map<String, Stat> combine(Map<String, Stat> left, Map<String, Stat> right) {
        right.forEach((code, stat) -> left.computeIfAbsent(code, unused -> new Stat()).merge(stat));
        return left;
    }

    @Override
    public List<String> header() {
        return List.of("Rank", "StockCode", "Description",
                "QuantitySold", "QuantityReturned", "QuantityNet", "RevenueNet");
    }

    @Override
    public List<List<String>> rows(Map<String, Stat> accumulator) {
        List<Map.Entry<String, Stat>> top = new ArrayList<>(accumulator.entrySet());
        top.sort(Comparator
                .comparingLong((Map.Entry<String, Stat> e) -> e.getValue().quantityNet()).reversed()
                .thenComparing(Map.Entry::getKey));

        List<List<String>> rows = new ArrayList<>(limit);
        for (int i = 0; i < Math.min(limit, top.size()); i++) {
            Map.Entry<String, Stat> entry = top.get(i);
            Stat stat = entry.getValue();
            rows.add(List.of(
                    String.valueOf(i + 1),
                    entry.getKey(),
                    stat.description,
                    String.valueOf(stat.quantitySold),
                    String.valueOf(stat.quantityReturned),
                    String.valueOf(stat.quantityNet()),
                    Money.format(stat.revenueNetScaled)));
        }
        return rows;
    }
}
