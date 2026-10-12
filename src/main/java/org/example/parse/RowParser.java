package org.example.parse;

import org.example.model.RawRow;

/** Превращает сырую строку в типизированный объект. */
public interface RowParser<T> {

    ParseResult<T> parse(RawRow row);
}
