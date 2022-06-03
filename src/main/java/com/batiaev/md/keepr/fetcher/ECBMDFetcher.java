package com.batiaev.md.keepr.fetcher;

import org.fintecy.md.ecb.EcbClient;
import org.fintecy.md.ecb.response.ExchangeRate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.time.ZoneOffset.UTC;

public class ECBMDFetcher implements MDFetcher {
    private static final String CREATE_TABLE_ECB_SQL =
            "CREATE TABLE IF NOT EXISTS ecb_rates(base varchar, counter varchar, moment timestamp, mid decimal, PRIMARY KEY(base, counter, moment))";
    private static final String INSERT_ECB_SQL =
            "INSERT OR REPLACE INTO ecb_rates(base, counter, moment, mid) VALUES (?, ?, ?, ?)";
    private final Connection connection;

    public ECBMDFetcher(String db) throws SQLException {
        this.connection = MDFetcher.connect(db);
        assert connection != null;
        connection.prepareStatement(createTableSql())
                .executeUpdate();
    }

    @Override
    public String createTableSql() {
        return CREATE_TABLE_ECB_SQL;
    }

    @Override
    public String insertSql() {
        return INSERT_ECB_SQL;
    }

    @Override
    public boolean fetch() {
        try {
            EcbClient.api()
                    .historicalRates()
                    .entrySet()
                    .stream()
//                    .sorted(Map.Entry.comparingByKey())//required inverted
                    .sorted((e1, e2) -> e2.getKey().compareTo(e1.getKey()))
                    .map(Map.Entry::getValue)
                    .forEach(this::process);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean fetch(LocalDate date) {
        try {
            return process(EcbClient.api().rates(date));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean process(List<ExchangeRate> rates) {
        try {
            var res = rates.stream()
                    .map(rate -> new com.batiaev.md.keepr.model.ExchangeRate(
                            rate.getBase().getCode(),
                            rate.getCounter().getCode(),
                            rate.getDate().atStartOfDay().toInstant(UTC),
                            rate.getMid()))
                    .collect(Collectors.toList());
            return insert(res);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean insert(Collection<com.batiaev.md.keepr.model.ExchangeRate> rates) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(insertSql());
            for (com.batiaev.md.keepr.model.ExchangeRate rate : rates) {
                System.out.println(rate);
                ps.setString(1, rate.base());
                ps.setString(2, rate.counter());
                ps.setLong(3, rate.moment().toEpochMilli());
                ps.setBigDecimal(4, rate.mid());
                ps.addBatch();
            }
            int[] ints = ps.executeBatch();
            System.out.println("Updates/inserted: " + ints);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            MDFetcher.close(ps);
        }
        return true;
    }
}
