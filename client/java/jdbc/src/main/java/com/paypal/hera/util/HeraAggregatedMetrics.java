package com.paypal.hera.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import static com.paypal.hera.constants.MicrometerConsts.*;

public class HeraAggregatedMetrics {
    static final ConcurrentHashMap<String, BoundLRUCaches<OCCJDBCMetrics>>
            occJdbcMetricsMap = new ConcurrentHashMap<>();

    private static String ifNullReturnUnknown(String value) {
        if (value == null)
            return "unknown";
        return value;
    }

    private static OCCJDBCMetrics checkAndGetMetrics(String dbHost, String queryHash) {
        if (occJdbcMetricsMap.containsKey(dbHost))
            return occJdbcMetricsMap.get(dbHost).get(queryHash);
        return null;
    }

    public static OCCJDBCMetrics getMetrics(String dbHost, String queryHash, int cacheSize) {

        dbHost = ifNullReturnUnknown(dbHost);
        queryHash = ifNullReturnUnknown(queryHash);

        OCCJDBCMetrics metrics = checkAndGetMetrics(dbHost, queryHash);

        if(metrics == null) {
            BoundLRUCaches<OCCJDBCMetrics> occJdbcMetricsToQuery = occJdbcMetricsMap.putIfAbsent(dbHost, new BoundLRUCaches<OCCJDBCMetrics>(cacheSize, dbHost));
            if(occJdbcMetricsToQuery == null)
                occJdbcMetricsToQuery = occJdbcMetricsMap.get(dbHost);
            OCCJDBCMetrics occjdbcMetrics = new OCCJDBCMetrics(queryHash);

            occjdbcMetrics.setExecFailCounter(Counter.builder(EXEC_FAIL_COUNT)
                    .description(EXEC_FAIL_COUNT_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("sql_hash", queryHash),
                            Tag.of("db_host", dbHost)))
                    .register(Metrics.globalRegistry));
            occjdbcMetrics.setExecSuccessCounter(Counter.builder(EXEC_SUCCESS_COUNT)
                    .description(EXEC_SUCCESS_COUNT_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("sql_hash", queryHash),
                            Tag.of("db_host", dbHost)))
                    .register(Metrics.globalRegistry));
            occjdbcMetrics.setFetchFailCounter(Counter.builder(FETCH_FAIL_COUNT)
                    .description(FETCH_FAIL_COUNT_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("sql_hash", queryHash),
                            Tag.of("db_host", dbHost)))
                    .register(Metrics.globalRegistry));
            occjdbcMetrics.setFetchSuccessCounter(Counter.builder(FETCH_SUCCESS_COUNT)
                    .description(FETCH_SUCCESS_COUNT_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("sql_hash", queryHash),
                            Tag.of("db_host", dbHost)))
                    .register(Metrics.globalRegistry));
            occjdbcMetrics.setExecTimer(Timer.builder(EXEC_TIMER)
                    .description(EXEC_TIMER_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("db_host", dbHost),
                            Tag.of("sql_hash", queryHash)))
                    .register(Metrics.globalRegistry));
            occjdbcMetrics.setFetchTimer(Timer.builder(FETCH_TIMER)
                    .description(FETCH_TIMER_DESCRIPTION)
                    .tags(Arrays.asList(Tag.of("db_host", dbHost),
                            Tag.of("sql_hash", queryHash)))
                    .register(Metrics.globalRegistry));
            occJdbcMetricsToQuery.putIfAbsent(queryHash, occjdbcMetrics);
            return occjdbcMetrics;
        } else {
            return metrics;
        }
    }
}
