package org.example;

import org.example.model.Money;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MoneyTest {

    @Test
    void keepsThreeDecimalsWithoutLoss() {
        assertEquals(1235L, Money.toScaled(new BigDecimal("0.1235")));
        assertEquals("0.13", Money.format(Money.toScaled(new BigDecimal("0.1250"))));
        assertEquals(new BigDecimal("2.5500"), Money.fromScaled(Money.toScaled(new BigDecimal("2.55"))));
    }

    @Test
    void sumsExactlyRegardlessOfOrder() {
        long[] prices = {Money.toScaled(new BigDecimal("0.01")), Money.toScaled(new BigDecimal("2.55")),
                Money.toScaled(new BigDecimal("38970"))};
        long forward = prices[0] + prices[1] + prices[2];
        long backward = prices[2] + prices[1] + prices[0];
        assertEquals(forward, backward);
        assertEquals("38972.56", Money.format(forward));
    }

    @Test
    void averageIsEmptyForZeroOrders() {
        assertEquals("", Money.formatAverage(100, 0));
        assertEquals("33.33", Money.formatAverage(Money.toScaled(new BigDecimal("100")), 3));
    }
}
