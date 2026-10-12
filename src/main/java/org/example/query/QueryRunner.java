package org.example.query;

/** Исполнитель запросов над подготовленным слоем. */
public interface QueryRunner {

    <A> A run(Query<A> query);

    String mode();
}
