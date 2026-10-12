package org.example.dq;

import org.example.model.InvoiceLine;

/**
 * Правила датасета Online Retail:
 * - Quantity > 0 и обычный счёт -> продажа;
 * - Quantity < 0 и счёт с префиксом C -> отмена, это возврат;
 * - Quantity < 0 без префикса C -> корректировка склада (брак, пересчёт), тоже возврат,
 *   но строка помечается предупреждением NegativeQuantityWithoutCancellation;
 * - Quantity > 0 на счёте-отмене -> противоречие, строка в reject;
 * - Quantity == 0 -> бессмысленная строка, в reject.
 *
 * Сами возвраты не удаляются: они нужны, чтобы считать выручку нетто.
 * Решение «вычитать возвраты или игнорировать» принимается на уровне агрегации
 * по полю PreparedLine.flow.
 */
public final class DefaultReturnPolicy implements ReturnPolicy {

    @Override
    public ReturnDecision decide(InvoiceLine line) {
        if (line.quantity() == 0) {
            return ReturnDecision.INVALID;
        }
        if (line.quantity() > 0) {
            return line.isCancellation() ? ReturnDecision.INVALID : ReturnDecision.SALE;
        }
        return ReturnDecision.RETURN;
    }
}
