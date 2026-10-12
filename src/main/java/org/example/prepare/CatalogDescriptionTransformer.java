package org.example.prepare;

import org.example.model.InvoiceLine;

/** Подменяет Description на каноничное из справочника. */
public final class CatalogDescriptionTransformer implements LineTransformer {

    private final ProductCatalog catalog;

    public CatalogDescriptionTransformer(ProductCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public InvoiceLine apply(InvoiceLine line) {
        String description = catalog.descriptionFor(line.stockCode(), line.description());
        return description.equals(line.description())
                ? line
                : new InvoiceLine(line.lineNumber(), line.invoiceNo(), line.stockCode(), description,
                line.quantity(), line.invoiceDate(), line.unitPrice(), line.customerId(), line.country());
    }
}
