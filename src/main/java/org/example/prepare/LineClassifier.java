package org.example.prepare;

import org.example.model.InvoiceLine;
import org.example.model.LineKind;

/** Определяет, товар это или служебная позиция. */
public interface LineClassifier {

    LineKind classify(InvoiceLine line);
}
