package org.example.dq;

import org.example.model.InvoiceLine;

/** Политика обработки возвратов и отмен. */
public interface ReturnPolicy {

    ReturnDecision decide(InvoiceLine line);
}
