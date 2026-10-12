package org.example.query;

import org.example.model.LineFlow;
import org.example.model.Money;
import org.example.model.PreparedLine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Запрос 2: по каждому клиенту число заказов, потраченная сумма и средний чек.
 * Служебные позиции (доставка, сборы) входят в деньги: клиент их реально заплатил.
 * Строки без CustomerID пропускаются, их 135080 и привязать их к клиенту нельзя.
 *
 * Gross считается только по продажам, netto учитывает возвраты.
 * Заказ - уникальный InvoiceNo; отмена (счёт C*) это отдельный документ,
 * поэтому в OrdersTotal она попадает, а в OrdersSales нет.
 */
public final class CustomerMetricsQuery implements Query<Map<String, CustomerMetricsQuery.Stat>> {

    public static final class Stat {
        final Set<String> salesInvoices = new HashSet<>();
        final Set<String> allInvoices = new HashSet<>();
        long grossScaled;
        long returnsScaled;

        long nettoScaled() {
            return grossScaled - returnsScaled;
        }

        void merge(Stat other) {
            salesInvoices.addAll(other.salesInvoices);
            allInvoices.addAll(other.allInvoices);
            grossScaled += other.grossScaled;
            returnsScaled += other.returnsScaled;
        }
    }

    @Override
    public String name() {
        return "customer_metrics";
    }

    @Override
    public Map<String, Stat> newAccumulator() {
        return new HashMap<>();
    }

    @Override
    public void accumulate(Map<String, Stat> accumulator, PreparedLine line) {
        if (line.customerId() == null) {
            return;
        }
        Stat stat = accumulator.computeIfAbsent(line.customerId(), unused -> new Stat());
        stat.allInvoices.add(line.invoiceNo());
        if (line.flow() == LineFlow.RETURN) {
            stat.returnsScaled += -Money.toScaled(line.totalPrice());
        } else {
            stat.salesInvoices.add(line.invoiceNo());
            stat.grossScaled += Money.toScaled(line.totalPrice());
        }
    }

    @Override
    public Map<String, Stat> combine(Map<String, Stat> left, Map<String, Stat> right) {
        right.forEach((customer, stat) -> left.computeIfAbsent(customer, unused -> new Stat()).merge(stat));
        return left;
    }

    @Override
    public List<String> header() {
        return List.of("CustomerID", "OrdersSales", "OrdersTotal",
                "TotalGross", "ReturnsAmount", "TotalNet", "AvgCheckGross", "AvgCheckNet");
    }

    @Override
    public List<List<String>> rows(Map<String, Stat> accumulator) {
        List<Map.Entry<String, Stat>> customers = new ArrayList<>(accumulator.entrySet());
        customers.sort(Comparator
                .comparingLong((Map.Entry<String, Stat> e) -> e.getValue().nettoScaled()).reversed()
                .thenComparing(Map.Entry::getKey));

        List<List<String>> rows = new ArrayList<>(customers.size());
        for (Map.Entry<String, Stat> entry : customers) {
            Stat stat = entry.getValue();
            rows.add(List.of(
                    entry.getKey(),
                    String.valueOf(stat.salesInvoices.size()),
                    String.valueOf(stat.allInvoices.size()),
                    Money.format(stat.grossScaled),
                    Money.format(stat.returnsScaled),
                    Money.format(stat.nettoScaled()),
                    Money.formatAverage(stat.grossScaled, stat.salesInvoices.size()),
                    Money.formatAverage(stat.nettoScaled(), stat.allInvoices.size())));
        }
        return rows;
    }
}
