package com.batiaev.md.keepr.model;

import java.math.BigDecimal;
import java.time.Instant;

public record ExchangeRate(String base, String counter, Instant moment, BigDecimal mid, BigDecimal bid,
                           BigDecimal ask) {
    public ExchangeRate(String base, String counter, Instant moment, BigDecimal mid) {
        this(base, counter, moment, mid, mid, mid);
    }
}
