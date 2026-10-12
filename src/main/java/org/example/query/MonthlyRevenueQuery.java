package org.example.query;

import org.example.model.LineFlow;
import org.example.model.Money;
import org.example.model.PreparedLine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Запрос 3: выручка, возвраты и их доля по месяцам. Нужен как перекрёстная
 * проверка: суммы по месяцам должны складываться в общий итог, а всплеск доли
 * возвратов в месяце - сигнал для DQ.
 */
public final class MonthlyRevenueQuery implements Query<Map<String, MonthlyRevenueQuery.Stat>> {

    public static final class Stat {
        final Set<String> invoices = new HashSet<>();
        long salesScaled;
        long returnsScaled;

        long nettoScaled() {
            return salesScaled - returnsScaled;
        }

        void merge(Stat other) {
            invoices.addAll(other.invoices);
            salesScaled += other.salesScaled;
            returnsScaled += other.returnsScaled;
        }
    }

    @Override
    public String name() {
        return "monthly_revenue";
    }

    @Override
    public Map<String, Stat> newAccumulator() {
        return new HashMap<>();
    }

    @Override
    public void accumulate(Map<String, Stat> accumulator, PreparedLine line) {
        String month = "%04d-%02d".formatted(line.invoiceDate().getYear(), line.invoiceDate().getMonthValue());
        Stat stat = accumulator.computeIfAbsent(month, unused -> new Stat());
        stat.invoices.add(line.invoiceNo());
        if (line.flow() == LineFlow.RETURN) {
            stat.returnsScaled += -Money.toScaled(line.totalPrice());
        } else {
            stat.salesScaled += Money.toScaled(line.totalPrice());
        }
    }

    @Override
    public Map<String, Stat> combine(Map<String, Stat> left, Map<String, Stat> right) {
        right.forEach((month, stat) -> left.computeIfAbsent(month, unused -> new Stat()).merge(stat));
        return left;
    }

    @Override
    public List<String> header() {
        return List.of("Month", "Orders", "RevenueSales", "Returns", "RevenueNet", "ReturnsSharePct");
    }

    @Override
    public List<List<String>> rows(Map<String, Stat> accumulator) {
        List<List<String>> rows = new ArrayList<>(accumulator.size());
        new TreeMap<>(accumulator).forEach((month, stat) -> rows.add(List.of(
                month,
                String.valueOf(stat.invoices.size()),
                Money.format(stat.salesScaled),
                Money.format(stat.returnsScaled),
                Money.format(stat.nettoScaled()),
                share(stat.returnsScaled, stat.salesScaled))));
        return rows;
    }

    private static String share(long returns, long sales) {
        if (sales == 0) {
            return "";
        }
        return BigDecimal.valueOf(returns)
                .multiply(BigDecimal.valueOf(100))
                .divide(BigDecimal.valueOf(sales), 2, RoundingMode.HALF_UP)
                .toPlainString();
    }
}
