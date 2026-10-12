package org.example.query;

import org.example.model.PreparedLine;

import java.util.List;

/**
 * Запрос в виде свёртки map-reduce. Такая форма позволяет считать один и тот же
 * запрос и в одном потоке, и параллельно по частям файла: аккумуляторы частей
 * сливаются через combine.
 *
 * @param <A> тип аккумулятора
 */
public interface Query<A> {

    String name();

    A newAccumulator();

    void accumulate(A accumulator, PreparedLine line);

    /** Слияние двух аккумуляторов. Должно быть ассоциативным. */
    A combine(A left, A right);

    List<String> header();

    List<List<String>> rows(A accumulator);
}
