package com.batiaev.md.keepr;

import com.batiaev.md.keepr.fetcher.*;
import com.batiaev.md.keepr.model.Source;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.*;
import java.util.*;

import static java.time.ZoneOffset.UTC;

public class KeeprApp {

    public static void main(String[] args) throws SQLException {
        var dbUrl = args[0];
        var source = Source.valueOf(args[1]);
        fetchCandles(dbUrl, args[2]);
//        checkMissingDates(dbUrl, source);
//        getRatesHistory(dbUrl, source);
//        fetch(dbUrl, source, args);
    }

    private static void fetchCandles(String dbUrl, String token) throws SQLException {
        new OXRMDFetcher(dbUrl, token).fetchCandles();
    }

    private static void getRatesHistory(String dbUrl, Source source) throws SQLException {
        var connection = MDFetcher.connect(dbUrl);
        String sourceTable = "origins";
        switch (source) {
            case CBR -> {
                sourceTable = "cbr_rates";
            }
            case ECB -> {
                sourceTable = "ecb_rates";
            }
            case OXR -> {
                sourceTable = "oxr_rates";
            }
            case REVOLUT -> {
                sourceTable = "revolut_rates";
            }
        }
        LocalDate startOfYear = LocalDate.of(Year.now().getValue(), 1, 1);
        var execute = connection.prepareStatement(
//                        "select base || '-' || counter as pair,  date(moment/1000, 'unixepoch') as value_date, mid" +
                "select moment, mid from " + sourceTable
                        + " where counter = 'GBP' and date(moment/1000, 'unixepoch') >= '" + startOfYear
                        + "' order by moment desc").executeQuery();

        SortedMap<LocalDate, BigDecimal> dates = new TreeMap<>();
        while (execute.next()) {

            Instant instant = Instant.ofEpochMilli(execute.getLong(1));
            LocalDate localDate = instant.atOffset(UTC).toLocalDate();
            dates.put(localDate, execute.getBigDecimal(2));
        }
        dates.forEach((localDate, midRate) -> {
            System.out.println(localDate + "=" + midRate);
        });
    }

    private static void fetch(String dbUrl, Source source, String[] args) throws SQLException {
        switch (source) {
            case CBR -> new CBRMDFetcher(dbUrl).fetch();
            case ECB -> new ECBMDFetcher(dbUrl).fetch();
            case OXR -> new OXRMDFetcher(dbUrl, args[2]).fetch();
            case REVOLUT -> new RevolutMDFetcher(dbUrl).fetch();
        }
    }

    private static void checkMissingDates(String dbUrl, Source source) throws SQLException {
        var connection = MDFetcher.connect(dbUrl);
        String sourceTable = "origins";
        switch (source) {
            case CBR -> {
                sourceTable = "cbr_rates";
            }
            case ECB -> {
                sourceTable = "ecb_rates";
            }
            case OXR -> {
                sourceTable = "oxr_rates";
            }
            case REVOLUT -> {
                sourceTable = "revolut_rates";
            }
        }
        var execute = connection.prepareStatement(
                "select distinct moment from " + sourceTable + " order by moment desc").executeQuery();

        List<LocalDate> missingDates = new ArrayList<>();
        LocalDate last = null;
        while (execute.next()) {
            Instant instant = Instant.ofEpochMilli(execute.getLong(1));
            LocalDate localDate = instant.atOffset(UTC).toLocalDate();
            if (last != null) {
                int days = Period.between(localDate, last).getDays();
                if (days > 1) {
                    for (int i = 1; i < days; i++) {
                        LocalDate ld2 = last.minusDays(i);
                        if (!ld2.getDayOfWeek().equals(DayOfWeek.SUNDAY)
                                && !ld2.getDayOfWeek().equals(DayOfWeek.SATURDAY))
                            missingDates.add(ld2);
                    }
                }
            }
            last = localDate;
        }
        System.out.println("missing " + missingDates.size() + " dates:");
        var missingPerDay = new HashMap<DayOfWeek, Integer>();
        for (LocalDate missingDate : missingDates) {
            DayOfWeek day = missingDate.getDayOfWeek();
            missingPerDay.put(day, missingPerDay.getOrDefault(day, 0) + 1);
        }
        System.out.println(missingPerDay);
        for (LocalDate missingDate : missingDates) {
            System.out.println(missingDate + " / " + missingDate.getDayOfWeek());
        }
    }
}
