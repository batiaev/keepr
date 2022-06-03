package com.batiaev.md.keepr;

import com.batiaev.md.keepr.fetcher.CBRMDFetcher;
import com.batiaev.md.keepr.fetcher.ECBMDFetcher;
import com.batiaev.md.keepr.fetcher.OXRMDFetcher;
import com.batiaev.md.keepr.fetcher.RevolutMDFetcher;
import com.batiaev.md.keepr.model.Source;

import java.sql.SQLException;

public class KeeprApp {

    public static void main(String[] args) throws SQLException {
        var dbUrl = args[0];
        var source = Source.valueOf(args[1]);
        switch (source) {
            case CBR -> new CBRMDFetcher(dbUrl).fetch();
            case ECB -> new ECBMDFetcher(dbUrl).fetch();
            case OXR -> new OXRMDFetcher(dbUrl, args[2]).fetch();
            case REVOLUT -> new RevolutMDFetcher(dbUrl).fetch();
        }
    }
}
