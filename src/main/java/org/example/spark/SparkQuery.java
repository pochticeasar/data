package org.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Запрос над подготовленным слоем. Имя задаёт имя файла выгрузки. */
public interface SparkQuery {

    String name();

    Dataset<Row> apply(Dataset<Row> prepared);
}
