package org.example.prepare;

/** Справочник товаров: один StockCode - одно каноничное название. */
public interface ProductCatalog {

    /** Каноничное описание кода; fallback, если кода нет в справочнике. */
    String descriptionFor(String stockCode, String fallback);

    int size();
}
