package com.batiaev.md.keepr.fetcher;

import com.batiaev.md.keepr.model.ExchangeRate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Collection;

public interface MDFetcher {
    String createTableSql();

    String insertSql();

    boolean fetch();

    boolean fetch(LocalDate date);

    boolean insert(Collection<ExchangeRate> rates);

    static Connection connect(String path) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            if (connection != null)
                System.out.println("Connected to db.");
            return connection;
        } catch (SQLException ex) {
            System.err.println("Couldn't connect." + ex.getMessage());
        }
        return null;
    }

    static void close(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
