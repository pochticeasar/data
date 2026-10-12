package org.example.dq;

/** Чем считать строку по знаку Quantity и типу счёта. */
public enum ReturnDecision {
    SALE,
    RETURN,
    /** Противоречие в данных, строку в агрегации пускать нельзя. */
    INVALID
}
