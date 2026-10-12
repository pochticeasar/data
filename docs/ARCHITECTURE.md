# Архитектура пайплайна на Spark

## Поток данных

```mermaid
flowchart LR
    CSV[("data.csv<br/>541 909 строк<br/>ISO-8859-1")]

    subgraph S1["Шаг 1: Ingest"]
        I["SparkPipeline.ingest()<br/>try_cast + try_to_timestamp"]
    end
    subgraph S2["Шаг 2: Data Quality"]
        P["SparkPipeline.prepare()<br/>18 правил, каталог, возвраты"]
    end
    subgraph S34["Шаги 3-4: Queries + Save"]
        Q["SparkQuery<br/>groupBy / agg"]
    end

    RAW[("raw.parquet<br/>541 909 строк")]
    PREP[("prepared.parquet<br/>536 637 строк<br/>14 партиций")]

    RJ1[["rejected_ingest.csv"]]
    RJ2[["rejected.csv"]]
    DQ[["dq_report.csv"]]
    R1[["top_products.csv"]]
    R2[["customer_metrics.csv"]]
    R3[["monthly_revenue.csv"]]

    CSV --> I --> RAW --> P --> PREP --> Q
    I -. битые типы .-> RJ1
    P -. нарушения ERROR .-> RJ2
    P -. счётчики правил .-> DQ
    Q --> R1 & R2 & R3
```

Почему два слоя parquet, а не один: правила качества меняются чаще, чем формат
источника. CSV разбирается один раз, дальше DQ-шаг читает колоночный
`raw.parquet` и его можно перезапускать с новыми правилами.

## Классы и интерфейсы

```mermaid
classDiagram
    direction LR

    class SparkQuery {
        <<interface>>
        +name() String
        +apply(Dataset~Row~) Dataset~Row~
    }
    class SparkRule {
        <<record>>
        +name String
        +severity Severity
        +violated Column
    }
    class StageResult {
        <<record>>
        +read long
        +accepted long
        +rejected long
        +violations Map
        +isBalanced() boolean
    }

    class SparkPipeline {
        +ingest() StageResult
        +prepare() StageResult
        +runQuery(SparkQuery) Path
    }

    TopProductsQuery ..|> SparkQuery
    CustomerMetricsQuery ..|> SparkQuery
    MonthlyRevenueQuery ..|> SparkQuery

    SparkPipeline --> SparkQuery
    SparkPipeline --> StageResult
    SparkPipeline --> Rules
    SparkPipeline --> Classifier
    SparkPipeline --> RetailSchema
    SparkPipeline --> CsvOutput
    Rules --> SparkRule
    SparkRule --> Severity
    Classifier --> LineKind
    Classifier --> LineFlow
```

Правило качества это не класс с методом, а запись с выражением `Column`.
Поэтому все 18 правил считаются одним проходом: причина отбраковки собирается
в цепочку `when`, а счётчики нарушений в один `agg`.

## Обработка строки на шаге DQ

```mermaid
sequenceDiagram
    participant RAW as raw.parquet
    participant CAT as Каталог товаров
    participant W as Оконная функция
    participant R as Rules.errors()
    participant C as Classifier
    participant OK as prepared.parquet
    participant BAD as rejected.csv

    RAW->>CAT: groupBy(stockCode, description).count()
    CAT-->>RAW: каноничное описание на код
    RAW->>W: row_number по полному ключу строки
    W-->>RAW: rowSeq (rowSeq > 1 это дубль)
    RAW->>R: цепочка when по ERROR-правилам
    R-->>RAW: rejectRule или null
    alt rejectRule заполнен
        RAW->>BAD: LineNumber, Rule, Payload
    else
        RAW->>C: lineKind и lineFlow
        C-->>RAW: PRODUCT / SHIPPING / ... и SALE / RETURN
        RAW->>OK: + totalPrice
    end
```

Дубли ищутся после нормализации описаний: иначе одна и та же строка с двумя
разными названиями товара дублем не считается.

## Параллелизм

```mermaid
flowchart TB
    CSV[("CSV / parquet")] --> SP["Spark разбивает вход на партиции"]
    SP --> T1["task 1"] & T2["task 2"] & TN["task N"]
    T1 & T2 & TN --> SH["shuffle по ключу группировки"]
    SH --> AG["агрегация частичных сумм"]
    AG --> OUT[["результат не зависит от числа партиций"]]
```

Параллелизм обеспечивает Spark: `local[*]` поднимает по задаче на ядро,
`spark.sql.shuffle.partitions` задаёт параллелизм агрегаций. Деньги считаются
в `decimal(20,4)`, а не в `double`, поэтому суммы точные и не зависят от
порядка слагаемых, то есть от разбиения на партиции.
