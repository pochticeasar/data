package org.example;

import org.example.dq.DefaultReturnPolicy;
import org.example.dq.ReturnDecision;
import org.example.dq.ReturnPolicy;
import org.example.model.InvoiceLine;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReturnPolicyTest {

    private final ReturnPolicy policy = new DefaultReturnPolicy();

    @Test
    void positiveQuantityOnPlainInvoiceIsSale() {
        assertEquals(ReturnDecision.SALE, policy.decide(line("536365", 6)));
    }

    @Test
    void negativeQuantityOnCancellationIsReturn() {
        assertEquals(ReturnDecision.RETURN, policy.decide(line("C536379", -1)));
    }

    @Test
    void negativeQuantityWithoutCancellationIsStillReturn() {
        assertEquals(ReturnDecision.RETURN, policy.decide(line("536589", -10)));
    }

    @Test
    void positiveQuantityOnCancellationIsInvalid() {
        assertEquals(ReturnDecision.INVALID, policy.decide(line("C536379", 3)));
    }

    @Test
    void zeroQuantityIsInvalid() {
        assertEquals(ReturnDecision.INVALID, policy.decide(line("536365", 0)));
    }

    private static InvoiceLine line(String invoiceNo, int quantity) {
        return new InvoiceLine(1, invoiceNo, "22197", "POPCORN HOLDER", quantity,
                LocalDateTime.of(2010, 12, 1, 8, 26), new BigDecimal("2.55"), "17850", "United Kingdom");
    }
}
