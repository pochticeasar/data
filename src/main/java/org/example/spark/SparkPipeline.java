package org.example.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.example.dq.Severity;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;
import static org.apache.spark.sql.functions.when;

/**
 * Четыре шага на Spark: загрузка CSV в parquet, правила качества, запросы, выгрузка.
 * Каждый шаг читает файл предыдущего, поэтому их можно запускать по отдельности.
 *
 * Деньги считаются в decimal, а не в double: суммы точные и не зависят от
 * порядка слагаемых, то есть от разбиения на партиции.
 */
public final class SparkPipeline {

    /** Бизнес-ключ полной строки, по нему ищутся дубли. */
    private static final String[] ROW_KEY = {
            "invoiceNo", "stockCode", "description", "quantity", "invoiceDate", "unitPrice", "customerId", "country"};

    private final SparkSession spark;
    private final Path input;
    private final Path outDir;

    public SparkPipeline(SparkSession spark, Path input, Path outDir) {
        this.spark = spark;
        this.input = input;
        this.outDir = outDir;
    }

    public Path rawParquet() {
        return outDir.resolve("raw.parquet");
    }

    public Path preparedParquet() {
        return outDir.resolve("prepared.parquet");
    }

    /** Шаг 1: CSV -> raw.parquet. Отбраковываются только технически битые строки. */
    public StageResult ingest() {
        long startedAt = System.currentTimeMillis();

        Dataset<Row> source = spark.read()
                .option("header", true)
                .option("encoding", RetailSchema.ENCODING)
                .schema(RetailSchema.SOURCE)
                .csv(input.toString())
                .withColumn("lineNumber", monotonically_increasing_id())
                .select(
                        col("lineNumber"),
                        trim(col("InvoiceNo")).as("invoiceNo"),
                        trim(col("StockCode")).as("stockCode"),
                        trim(col("Description")).as("description"),
                        trim(col("Quantity")).as("quantityRaw"),
                        trim(col("InvoiceDate")).as("invoiceDateRaw"),
                        trim(col("UnitPrice")).as("unitPriceRaw"),
                        trim(col("CustomerID")).as("customerIdRaw"),
                        trim(col("Country")).as("country"))
                // try_cast, а не cast: в Spark 4 включён ANSI-режим, и обычное
                // приведение не возвращает null, а валит весь job на первой битой строке.
                .withColumn("quantity", expr("try_cast(quantityRaw as int)"))
                .withColumn("unitPrice", expr("try_cast(unitPriceRaw as " + RetailSchema.MONEY_TYPE + ")"))
                .withColumn("invoiceDate",
                        expr("try_to_timestamp(invoiceDateRaw, '" + RetailSchema.DATE_FORMAT + "')"))
                .withColumn("customerId",
                        when(length(col("customerIdRaw")).equalTo(0), lit(null)).otherwise(col("customerIdRaw")))
                .withColumn("parseError", when(col("quantity").isNull(), lit("parse.quantity"))
                        .when(col("unitPrice").isNull(), lit("parse.unitPrice"))
                        .when(col("invoiceDate").isNull(), lit("parse.invoiceDate")))
                .cache();

        long read = source.count();
        Dataset<Row> broken = source.filter(col("parseError").isNotNull());
        long rejected = broken.count();

        CsvOutput.writeSingle(broken.select(
                col("lineNumber").as("LineNumber"),
                col("parseError").as("Rule"),
                concat_ws("/", col("invoiceNo"), col("stockCode")).as("Payload")),
                outDir.resolve("rejected_ingest.csv"));

        source.filter(col("parseError").isNull())
                .select("lineNumber", "invoiceNo", "stockCode", "description",
                        "quantity", "invoiceDate", "unitPrice", "customerId", "country")
                .write().mode(SaveMode.Overwrite).parquet(rawParquet().toString());

        source.unpersist();
        return new StageResult(read, read - rejected, rejected, Map.of(), System.currentTimeMillis() - startedAt);
    }

    /** Шаг 2: raw.parquet -> prepared.parquet, rejected.csv, dq_report.csv. */
    public StageResult prepare() {
        long startedAt = System.currentTimeMillis();

        Dataset<Row> raw = spark.read().parquet(rawParquet().toString());
        Dataset<Row> normalized = withCanonicalDescription(raw)
                .withColumn("isCancellation", upper(substring(col("invoiceNo"), 1, 1)).equalTo("C"));

        // Дубли ищутся после нормализации описаний: иначе один и тот же товар
        // с двумя разными названиями не считается дублем.
        WindowSpec duplicates = Window.partitionBy(columns(ROW_KEY)).orderBy(col("lineNumber"));
        Dataset<Row> marked = normalized
                .withColumn("rowSeq", row_number().over(duplicates))
                .withColumn("rejectRule", rejectRule())
                .cache();

        List<SparkRule> all = new ArrayList<>(Rules.errors());
        all.addAll(Rules.warnings());

        List<Column> aggregates = new ArrayList<>();
        aggregates.add(count(lit(1)).as("rows.read"));
        aggregates.add(sum(when(col("rejectRule").isNotNull(), 1).otherwise(0)).as("rows.rejected"));
        all.forEach(rule -> aggregates.add(
                sum(when(rule.violated(), 1).otherwise(0)).as(rule.name())));

        Row counters = marked.agg(aggregates.get(0), aggregates.subList(1, aggregates.size()).toArray(new Column[0])).first();
        long read = counters.getLong(counters.fieldIndex("rows.read"));
        long rejected = counters.getLong(counters.fieldIndex("rows.rejected"));

        Map<String, Long> violations = new LinkedHashMap<>();
        Map<String, Severity> severities = new LinkedHashMap<>();
        for (SparkRule rule : all) {
            long value = counters.getLong(counters.fieldIndex(rule.name()));
            if (value > 0) {
                violations.put(rule.name(), value);
                severities.put(rule.name(), rule.severity());
            }
        }

        CsvOutput.writeSingle(marked.filter(col("rejectRule").isNotNull()).select(
                col("lineNumber").as("LineNumber"),
                col("rejectRule").as("Rule"),
                concat_ws("/", col("invoiceNo"), col("stockCode")).as("Payload")),
                outDir.resolve("rejected.csv"));

        marked.filter(col("rejectRule").isNull())
                .withColumn("totalPrice",
                        col("unitPrice").multiply(col("quantity")).cast(RetailSchema.MONEY_TYPE))
                .withColumn("lineKind", Classifier.lineKind())
                .withColumn("lineFlow", Classifier.lineFlow())
                .select("invoiceNo", "stockCode", "description", "quantity", "invoiceDate",
                        "unitPrice", "totalPrice", "customerId", "country", "lineKind", "lineFlow")
                .write().mode(SaveMode.Overwrite).parquet(preparedParquet().toString());

        writeReport(read, read - rejected, rejected, violations, severities);
        marked.unpersist();
        return new StageResult(read, read - rejected, rejected, violations, System.currentTimeMillis() - startedAt);
    }

    /** Шаги 3 и 4: запрос по подготовленному слою и выгрузка результата. */
    public Path runQuery(SparkQuery query) {
        Dataset<Row> prepared = spark.read().parquet(preparedParquet().toString());
        Dataset<Row> result = query.apply(prepared);
        Path target = outDir.resolve(query.name() + ".csv");
        CsvOutput.writeSingle(result, target);
        result.show(5, 40);
        return target;
    }

    public long preparedRowCount() {
        return spark.read().parquet(preparedParquet().toString()).count();
    }

    public int preparedFiles() {
        return spark.read().parquet(preparedParquet().toString()).rdd().getNumPartitions();
    }

    /**
     * У 647 кодов из 4070 несколько разных Description. Берём самое частое
     * непустое описание, при равенстве - лексикографически меньшее, чтобы
     * прогон был воспроизводимым.
     */
    private Dataset<Row> withCanonicalDescription(Dataset<Row> raw) {
        WindowSpec byFrequency = Window.partitionBy(col("stockCode"))
                .orderBy(desc("count"), col("description").asc());

        Dataset<Row> canonical = raw
                .filter(length(col("description")).gt(0).and(length(col("stockCode")).gt(0)))
                .groupBy(col("stockCode"), col("description")).count()
                .withColumn("rank", row_number().over(byFrequency))
                .filter(col("rank").equalTo(1))
                .select(col("stockCode").as("catalogCode"), col("description").as("canonicalDescription"));

        return raw.join(canonical, col("stockCode").equalTo(col("catalogCode")), "left")
                .withColumn("description", coalesce(col("canonicalDescription"), col("description")))
                .drop("catalogCode", "canonicalDescription");
    }

    /** Причина отбраковки: первое сработавшее ERROR-правило в порядке приоритета. */
    private Column rejectRule() {
        List<SparkRule> errors = Rules.errors();
        Column chain = when(errors.get(0).violated(), lit(errors.get(0).name()));
        for (SparkRule rule : errors.subList(1, errors.size())) {
            chain = chain.when(rule.violated(), lit(rule.name()));
        }
        return chain;
    }

    private void writeReport(long read, long accepted, long rejected,
                             Map<String, Long> violations, Map<String, Severity> severities) {
        List<Row> rows = new ArrayList<>();
        rows.add(org.apache.spark.sql.RowFactory.create("rows.read", "INFO", read));
        rows.add(org.apache.spark.sql.RowFactory.create("rows.accepted", "INFO", accepted));
        rows.add(org.apache.spark.sql.RowFactory.create("rows.rejected", "INFO", rejected));
        violations.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .forEach(entry -> rows.add(org.apache.spark.sql.RowFactory.create(
                        entry.getKey(), severities.get(entry.getKey()).name(), entry.getValue())));

        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("Rule", org.apache.spark.sql.types.DataTypes.StringType)
                .add("Severity", org.apache.spark.sql.types.DataTypes.StringType)
                .add("Rows", org.apache.spark.sql.types.DataTypes.LongType);

        CsvOutput.writeSingle(spark.createDataFrame(rows, schema), outDir.resolve("dq_report.csv"));
    }

    private static Column[] columns(String[] names) {
        Column[] result = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            result[i] = col(names[i]);
        }
        return result;
    }
}
