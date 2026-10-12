package org.example.prepare;

/** Шаг подготовки данных: читает сырое, пишет готовое к агрегации. */
public interface DataPreparation {

    PreparationResult run();
}
