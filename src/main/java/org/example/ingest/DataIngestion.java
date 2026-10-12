package org.example.ingest;

/** Шаг 1: сырой файл в колоночный слой. Бизнес-правила здесь не применяются. */
public interface DataIngestion {

    IngestResult run();
}
