package org.example.model;

/** Тип позиции в счёте. В топ-5 товаров должен попадать только PRODUCT. */
public enum LineKind {
    PRODUCT,
    SHIPPING,
    FEE,
    ADJUSTMENT,
    GIFT_CARD,
    SAMPLE,
    OTHER
}
