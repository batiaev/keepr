package com.batiaev.md.keepr.fetcher;

import com.batiaev.md.keepr.model.ExchangeRate;
import org.fintecy.md.revolut.RevolutClient;
import org.fintecy.md.revolut.model.Currency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Year;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class RevolutMDFetcher implements MDFetcher {
    private final Logger log = LoggerFactory.getLogger(RevolutMDFetcher.class);

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS revolut_rates(base varchar, counter varchar, moment timestamp, mid decimal, bid decimal, ask decimal, PRIMARY KEY(base, counter, moment))";
    private static final String INSERT_SQL =
            "INSERT OR REPLACE INTO revolut_rates(base, counter, moment, mid, bid, ask) VALUES (?, ?, ?, ?, ?, ?)";
    private final Connection connection;

    public RevolutMDFetcher(String dbUrl) throws SQLException {
        this.connection = MDFetcher.connect(dbUrl);
        assert connection != null;
        connection.prepareStatement(createTableSql())
                .executeUpdate();
    }

    @Override
    public String createTableSql() {
        return CREATE_TABLE_SQL;
    }

    @Override
    public String insertSql() {
        return INSERT_SQL;
    }

    @Override
    public boolean fetch() {
        LocalDate date = LocalDate.now();
        while (date.isAfter(LocalDate.of(Year.now().getValue(), 1, 1))) {
            if (!fetch(date))
                return false;
            date = date.minusDays(1);
        }
        return true;
    }

    @Override
    public boolean fetch(LocalDate date) {
        try {
            var currencies = RevolutClient.api()
                    .currencies().get();
            Currency usd = Currency.currency("USD");
            insert(currencies.parallelStream()
                    .filter(currency -> !currency.getCode().equalsIgnoreCase("CNX"))
                    .map(currency -> convert(usd, currency))
                    .filter(Objects::nonNull)
                    .toList());
            return true;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
        }
    }

    private ExchangeRate convert(Currency usd, Currency currency) {
        try {
            return RevolutClient.api()
                    .latest(usd, currency)
                    .thenApply(rate -> {
                        log.trace(String.format("%s/%s = %s", usd.getCode(), currency.getCode(), rate.getMid()));
                        return rate;
                    })
                    .thenApply(rate -> new ExchangeRate(
                            usd.getCode(),
                            currency.getCode(),
                            Instant.now(),
                            rate.getMid(),
                            rate.getBid(),
                            rate.getAsk()
                    ))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            log.error(String.format("Not able to process %s/%s at %s: %s", usd.getCode(), currency.getCode(), Instant.now(), e.getMessage()));
            return ExchangeRate.zero(usd, currency);
        }
    }

    @Override
    public boolean insert(Collection<ExchangeRate> rates) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(insertSql());
            for (ExchangeRate rate : rates) {
                System.out.println(rate);
                ps.setString(1, rate.base());
                ps.setString(2, rate.counter());
                ps.setLong(3, rate.moment().toEpochMilli());
                ps.setBigDecimal(4, rate.mid());
                ps.setBigDecimal(5, rate.bid());
                ps.setBigDecimal(6, rate.ask());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            MDFetcher.close(ps);
        }
        return true;
    }
}
