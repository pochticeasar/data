package org.example.prepare;

import org.example.model.InvoiceLine;

/** Нормализация строки перед проверками и выгрузкой. */
public interface LineTransformer {

    InvoiceLine apply(InvoiceLine line);
}
