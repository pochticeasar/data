# Архитектура пайплайна

## Поток данных

```mermaid
flowchart LR
    CSV[("data.csv<br/>541 909 строк<br/>ISO-8859-1")]

    subgraph S1["Шаг 1: Ingest"]
        I["CsvToParquetIngestion"]
    end
    subgraph S2["Шаг 2: Data Quality"]
        P["PreparationPipeline"]
    end
    subgraph S34["Шаги 3-4: Queries + Save"]
        Q["QueryRunner<br/>sequential | parallel"]
    end

    RAW[("raw.parquet<br/>541 909 строк")]
    PREP[("prepared.parquet<br/>536 637 строк<br/>62 row group")]

    RJ1[["rejected_ingest.csv"]]
    RJ2[["rejected.csv"]]
    DQ[["dq_report.csv"]]
    R1[["top_products.csv"]]
    R2[["customer_metrics.csv"]]
    R3[["monthly_revenue.csv"]]

    CSV --> I --> RAW --> P --> PREP --> Q
    I -. битые строки .-> RJ1
    P -. нарушения ERROR .-> RJ2
    P -. счётчики правил .-> DQ
    Q --> R1 & R2 & R3
```

Почему два слоя parquet, а не один: правила качества меняются чаще, чем формат
источника. Разбор CSV делается один раз, дальше DQ-прогон читает колоночный
`raw.parquet` и его можно перезапускать с новыми правилами, не трогая CSV.

## Классы и интерфейсы

```mermaid
classDiagram
    direction LR

    class RowSource {
        <<interface>>
        +header() List~String~
        +rows() Stream~RawRow~
    }
    class RecordSource~T~ {
        <<interface>>
        +records() Stream~T~
    }
    class RecordSink~T~ {
        <<interface>>
        +write(T record)
        +written() long
    }
    class RowParser~T~ {
        <<interface>>
        +parse(RawRow) ParseResult~T~
    }
    class ParquetMapper~T~ {
        <<interface>>
        +schema() Schema
        +toRecord(T) GenericRecord
        +fromRecord(GenericRecord) T
    }

    CsvRowSource ..|> RowSource
    ParquetRecordSource ..|> RecordSource
    CsvRecordSink ..|> RecordSink
    ParquetRecordSink ..|> RecordSink
    InvoiceLineParser ..|> RowParser
    RawLineMapper ..|> ParquetMapper
    PreparedLineMapper ..|> ParquetMapper
    ParquetRecordSink --> ParquetMapper
    ParquetRecordSource --> ParquetMapper

    class QualityRule {
        <<interface>>
        +name() String
        +severity() Severity
        +check(InvoiceLine) Optional~String~
    }
    class ReturnPolicy {
        <<interface>>
        +decide(InvoiceLine) ReturnDecision
    }
    class LineClassifier {
        <<interface>>
        +classify(InvoiceLine) LineKind
    }
    class LineTransformer {
        <<interface>>
        +apply(InvoiceLine) InvoiceLine
    }
    class ProductCatalog {
        <<interface>>
        +descriptionFor(code, fallback) String
        +size() int
    }
    class QualityReport {
        <<interface>>
        +onRead()
        +onAccepted()
        +onRejected(rule)
        +onViolation(rule, severity)
    }

    SimpleRule ..|> QualityRule
    DuplicateRowRule ..|> QualityRule
    DefaultReturnPolicy ..|> ReturnPolicy
    StockCodeClassifier ..|> LineClassifier
    CatalogDescriptionTransformer ..|> LineTransformer
    CatalogDescriptionTransformer --> ProductCatalog
    MostFrequentDescriptionCatalog ..|> ProductCatalog
    InMemoryQualityReport ..|> QualityReport

    class DataIngestion {
        <<interface>>
        +run() IngestResult
    }
    class DataPreparation {
        <<interface>>
        +run() PreparationResult
    }
    class Query~A~ {
        <<interface>>
        +name() String
        +newAccumulator() A
        +accumulate(A, PreparedLine)
        +combine(A, A) A
        +header() List~String~
        +rows(A) List
    }
    class QueryRunner {
        <<interface>>
        +run(Query~A~) A
        +mode() String
    }

    CsvToParquetIngestion ..|> DataIngestion
    PreparationPipeline ..|> DataPreparation
    SequentialQueryRunner ..|> QueryRunner
    ParallelQueryRunner ..|> QueryRunner
    TopProductsQuery ..|> Query
    CustomerMetricsQuery ..|> Query
    MonthlyRevenueQuery ..|> Query

    CsvToParquetIngestion --> RowSource
    CsvToParquetIngestion --> RowParser
    CsvToParquetIngestion --> RecordSink
    PreparationPipeline --> RecordSource
    PreparationPipeline --> LineTransformer
    PreparationPipeline --> QualityRule
    PreparationPipeline --> ReturnPolicy
    PreparationPipeline --> LineClassifier
    PreparationPipeline --> RecordSink
    PreparationPipeline --> QualityReport
    SequentialQueryRunner --> RecordSource
    ParallelQueryRunner --> RecordSource
    ParallelQueryRunner --> RowGroups

    Pipeline --> DataIngestion
    Pipeline --> DataPreparation
    Pipeline --> QueryRunner
```

## Обработка строки на шаге DQ

```mermaid
sequenceDiagram
    participant S as ParquetRecordSource
    participant P as PreparationPipeline
    participant T as LineTransformer
    participant R as QualityRule[]
    participant RP as ReturnPolicy
    participant C as LineClassifier
    participant OK as prepared.parquet
    participant BAD as rejected.csv

    S->>P: InvoiceLine
    P->>T: нормализовать Description по каталогу
    T-->>P: InvoiceLine
    P->>R: проверить все правила
    R-->>P: нарушения (ERROR / WARNING)
    alt есть ERROR
        P->>BAD: Rejection(правило, причина)
    else только WARNING или чисто
        P->>RP: продажа или возврат?
        RP-->>P: SALE / RETURN / INVALID
        alt INVALID
            P->>BAD: Rejection(returns.invalid)
        else
            P->>C: тип позиции
            C-->>P: PRODUCT / SHIPPING / FEE / ...
            P->>OK: PreparedLine(TotalPrice, kind, flow)
        end
    end
```

## Параллельное исполнение запросов

```mermaid
flowchart TB
    PREP[("prepared.parquet<br/>62 row group")]
    PREP --> R1["ByteRange 1"] --> A1["аккумулятор 1"]
    PREP --> R2["ByteRange 2"] --> A2["аккумулятор 2"]
    PREP --> RN["ByteRange N"] --> AN["аккумулятор N"]
    A1 --> M["combine<br/>ассоциативное слияние"]
    A2 --> M
    AN --> M
    M --> OUT[["результат = результату одного потока"]]
```

`Query` задан как map-reduce свёртка: `newAccumulator`, `accumulate`, `combine`.
Поэтому один и тот же запрос исполняется и в одном потоке, и по row group
параллельно. Разделяемого состояния нет, блокировок нет, слияние один раз в конце.
`Main` прогоняет каждый запрос оба раза и сверяет результаты между собой.
