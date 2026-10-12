package org.example.model;

/** Направление движения товара. */
public enum LineFlow {
    /** Продажа, Quantity > 0. */
    SALE,
    /** Возврат или отмена, Quantity < 0. */
    RETURN
}
