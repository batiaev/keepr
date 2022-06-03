package com.batiaev.md.keepr.fetcher;

import com.batiaev.md.keepr.model.ExchangeRate;
import org.fintecy.md.cbr.CbrClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Year;
import java.util.Collection;

import static java.time.ZoneOffset.UTC;

public class CBRMDFetcher implements MDFetcher {
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS cbr_rates(base varchar, counter varchar, moment timestamp, mid decimal, PRIMARY KEY(base, counter, moment))";
    private static final String INSERT_SQL =
            "INSERT OR REPLACE INTO cbr_rates(base, counter, moment, mid) VALUES (?, ?, ?, ?)";
    private final Connection connection;

    public CBRMDFetcher(String dbUrl) throws SQLException {
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
        return insert(CbrClient.api()
                .rates(date)
                .stream()
                .map(exchangeRate -> new ExchangeRate(
                        exchangeRate.getBase().getCode(),
                        exchangeRate.getCounter().getCode(),
                        exchangeRate.getDate().atStartOfDay().toInstant(UTC),
                        exchangeRate.getMid()))
                .toList());
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
