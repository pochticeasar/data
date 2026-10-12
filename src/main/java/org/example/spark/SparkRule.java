package org.example.spark;

import org.apache.spark.sql.Column;
import org.example.dq.Severity;

/**
 * Правило качества в виде выражения Spark. Условие истинно, когда строка
 * правило нарушает. ERROR уводит строку в reject, WARNING только считается.
 */
public record SparkRule(String name, Severity severity, Column violated) {
}
