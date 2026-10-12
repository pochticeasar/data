package org.example.prepare;

import org.example.dq.QualityReport;
import org.example.dq.QualityRule;
import org.example.dq.ReturnDecision;
import org.example.dq.ReturnPolicy;
import org.example.dq.Severity;
import org.example.io.RecordSink;
import org.example.io.RowSource;
import org.example.model.InvoiceLine;
import org.example.model.LineFlow;
import org.example.model.LineKind;
import org.example.model.PreparedLine;
import org.example.model.RawRow;
import org.example.model.Rejection;
import org.example.parse.ParseResult;
import org.example.parse.RowParser;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Порядок обработки строки: парсинг -> нормализация -> правила качества ->
 * политика возвратов -> классификация позиции -> выгрузка.
 * Строка либо попадает в prepared, либо в rejected с причиной, третьего нет.
 */
public final class PreparationPipeline implements DataPreparation {

    private final Supplier<RowSource> sourceFactory;
    private final RowParser<InvoiceLine> parser;
    private final LineTransformer transformer;
    private final List<QualityRule> rules;
    private final ReturnPolicy returnPolicy;
    private final LineClassifier classifier;
    private final RecordSink<PreparedLine> preparedSink;
    private final RecordSink<Rejection> rejectedSink;
    private final QualityReport report;
    private final int catalogSize;

    public PreparationPipeline(
            Supplier<RowSource> sourceFactory,
            RowParser<InvoiceLine> parser,
            LineTransformer transformer,
            List<QualityRule> rules,
            ReturnPolicy returnPolicy,
            LineClassifier classifier,
            RecordSink<PreparedLine> preparedSink,
            RecordSink<Rejection> rejectedSink,
            QualityReport report,
            int catalogSize
    ) {
        this.sourceFactory = sourceFactory;
        this.parser = parser;
        this.transformer = transformer;
        this.rules = rules;
        this.returnPolicy = returnPolicy;
        this.classifier = classifier;
        this.preparedSink = preparedSink;
        this.rejectedSink = rejectedSink;
        this.report = report;
        this.catalogSize = catalogSize;
    }

    @Override
    public PreparationResult run() {
        try (RowSource source = sourceFactory.get()) {
            source.rows().forEach(this::process);
        }
        return new PreparationResult(
                report.read(),
                report.accepted(),
                report.rejected(),
                catalogSize,
                report.violations(),
                report.severities());
    }

    private void process(RawRow row) {
        report.onRead();

        InvoiceLine line;
        switch (parser.parse(row)) {
            case ParseResult.Ok<InvoiceLine> ok -> line = ok.value();
            case ParseResult.Error<InvoiceLine> error -> {
                report.onViolation(error.rule(), Severity.ERROR);
                reject(row, error.rule(), error.message());
                return;
            }
        }

        line = transformer.apply(line);

        List<String> blocking = new ArrayList<>(2);
        String blockingRule = null;
        for (QualityRule rule : rules) {
            var violation = rule.check(line);
            if (violation.isEmpty()) {
                continue;
            }
            report.onViolation(rule.name(), rule.severity());
            if (rule.severity() == Severity.ERROR) {
                if (blockingRule == null) {
                    blockingRule = rule.name();
                }
                blocking.add(rule.name() + ": " + violation.get());
            }
        }
        if (blockingRule != null) {
            reject(row, blockingRule, String.join("; ", blocking));
            return;
        }

        ReturnDecision decision = returnPolicy.decide(line);
        if (decision == ReturnDecision.INVALID) {
            report.onViolation("returns.invalid", Severity.ERROR);
            reject(row, "returns.invalid",
                    "Quantity = " + line.quantity() + ", счёт " + line.invoiceNo());
            return;
        }

        LineKind kind = classifier.classify(line);
        LineFlow flow = decision == ReturnDecision.RETURN ? LineFlow.RETURN : LineFlow.SALE;
        BigDecimal total = line.totalPrice().setScale(2, RoundingMode.HALF_UP);

        preparedSink.write(new PreparedLine(
                line.invoiceNo(),
                line.stockCode(),
                line.description(),
                line.quantity(),
                line.invoiceDate(),
                line.unitPrice(),
                total,
                line.customerId(),
                line.country(),
                kind,
                flow));
        report.onAccepted();
    }

    private void reject(RawRow row, String rule, String message) {
        rejectedSink.write(new Rejection(row.lineNumber(), rule, message, row.rawLine()));
        report.onRejected(rule);
    }
}
