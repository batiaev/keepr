package com.batiaev.md.keepr.model;

import org.fintecy.md.revolut.model.Currency;

import java.math.BigDecimal;
import java.time.Instant;

import static java.math.BigDecimal.ZERO;

public record ExchangeRate(String base, String counter, Instant moment, BigDecimal mid, BigDecimal bid,
                           BigDecimal ask) {
    public ExchangeRate(String base, String counter, Instant moment, BigDecimal mid) {
        this(base, counter, moment, mid, mid, mid);
    }

    public static ExchangeRate zero(Currency usd, Currency currency) {
        return new ExchangeRate(usd.getCode(), currency.getCode(), Instant.now(), ZERO, ZERO, ZERO);
    }
}
