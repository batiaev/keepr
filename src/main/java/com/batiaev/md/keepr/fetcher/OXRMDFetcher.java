package com.batiaev.md.keepr.fetcher;

import com.batiaev.md.keepr.model.ExchangeRate;
import org.fintecy.md.common.model.Currency;
import org.fintecy.md.oxr.OxrClient;
import org.fintecy.md.oxr.model.CandleStick;
import org.fintecy.md.oxr.model.OhlcResponse;
import org.fintecy.md.oxr.model.OxrPeriod;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.ZoneOffset.UTC;
import static org.fintecy.md.oxr.requests.RequestParamsFactory.ohlcParams;

public class OXRMDFetcher implements MDFetcher {
    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS oxr_rates(base varchar, counter varchar, moment timestamp, mid decimal, bid decimal, ask decimal,  PRIMARY KEY(base, counter, moment))";
    private static final String INSERT_SQL =
            "INSERT OR REPLACE INTO oxr_rates(base, counter, moment, mid, bid, ask) VALUES (?, ?, ?, ?, ?, ?)";
    private static final String CREATE_CANDLE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS oxr_candle_1d(base varchar, counter varchar, start timestamp, period varchar, open decimal, high decimal, low decimal, close decimal, average decimal, PRIMARY KEY(base, counter, start, period))";
    private static final String CREATE_CANDLE_30M_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS oxr_candle_30m(base varchar, counter varchar, start timestamp, period varchar, open decimal, high decimal, low decimal, close decimal, average decimal, PRIMARY KEY(base, counter, start, period))";
    private static final String INSERT_CANDLE_SQL =
            "INSERT OR REPLACE INTO oxr_candle_1d(base, counter, start, period, open, high, low, close, average) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_CANDLE_30M_SQL =
            "INSERT OR REPLACE INTO oxr_candle_30m(base, counter, start, period, open, high, low, close, average) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final Connection connection;
    private final String token;

    public OXRMDFetcher(String dbUrl, String token) throws SQLException {
        this.connection = MDFetcher.connect(dbUrl);
        this.token = token;
        assert connection != null;
        connection.prepareStatement(createTableSql())
                .executeUpdate();
        connection.prepareStatement(CREATE_CANDLE_TABLE_SQL)
                .executeUpdate();
        connection.prepareStatement(CREATE_CANDLE_30M_TABLE_SQL)
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

    public boolean fetchCandles() {
        LocalDateTime startDate = LocalDate.parse("2022-10-01").atStartOfDay();
        LocalDateTime date = LocalDate.now().atTime(14, 0);
        while (!date.isBefore(startDate)) {
            try {
                Instant start = date.toInstant(UTC);
                OxrClient.authorize(token)
                        .ohlc(ohlcParams(start, OxrPeriod.MIN_30)
                                .showBidAsk(true)
                                .build())
                        .thenApply(candles -> store(start, candles))
                        .get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
//                return false;
            }
            date = date.minusMinutes(30);
        }
        return true;
    }

    private boolean store(Instant start, OhlcResponse candles) {
        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(INSERT_CANDLE_30M_SQL);
            for (Map.Entry<Currency, CandleStick> entry : candles.getRates().entrySet()) {
                Currency currency = entry.getKey();
                CandleStick candleStick = entry.getValue();
                System.out.printf("%s=%s%n", currency.getCode(), candleStick);
                ps.setString(1, "USD");
                ps.setString(2, currency.getCode());
                ps.setLong(3, start.toEpochMilli());
                ps.setString(4, "30M");
                ps.setBigDecimal(5, candleStick.getOpen());
                ps.setBigDecimal(6, candleStick.getHigh());
                ps.setBigDecimal(7, candleStick.getLow());
                ps.setBigDecimal(8, candleStick.getClose());
                ps.setBigDecimal(9, candleStick.getAverage());
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
//            return false;
        } finally {
            MDFetcher.close(ps);
        }
        return true;
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
                            .map(e -> new ExchangeRate(
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
