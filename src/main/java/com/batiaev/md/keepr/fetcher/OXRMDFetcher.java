package com.batiaev.md.keepr.fetcher;

import com.batiaev.md.keepr.model.ExchangeRate;
import org.fintecy.md.oxr.OxrClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Year;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class OXRMDFetcher implements MDFetcher {
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS oxr_rates(base varchar, counter varchar, moment timestamp, mid decimal, bid decimal, ask decimal,  PRIMARY KEY(base, counter, moment))";
    private static final String INSERT_SQL =
            "INSERT OR REPLACE INTO oxr_rates(base, counter, moment, mid, bid, ask) VALUES (?, ?, ?, ?, ?, ?)";
    private final Connection connection;
    private final String token;

    public OXRMDFetcher(String dbUrl, String token) throws SQLException {
        this.connection = MDFetcher.connect(dbUrl);
        this.token = token;
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
            return insert(OxrClient.authorize(token)
                    .historical(date)
                    .thenApply(ratesResponse -> ratesResponse.getRates().entrySet()
                            .stream()
                            .map(e-> new ExchangeRate(
                                    ratesResponse.getBase().getCode(),
                                    e.getKey().getCode(),
                                    ratesResponse.getTimestamp(),
                                    e.getValue().getMid(),
                                    e.getValue().getBid(),
                                    e.getValue().getAsk()
                                    ))
                            .toList())
                    .get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return false;
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
