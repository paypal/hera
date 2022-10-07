package com.paypal.hera.constants;

public class MicrometerConsts {
    public static final String EXEC_TIMER = "pp.hera.dal.exec";
    public static final String EXEC_TIMER_DESCRIPTION = "This metric records the total duration of an EXEC transaction of a certain SQL. It can also describe the max, average, 50th, 90th and 99th percentile of all EXEC transaction durations.";
    public static final String FETCH_TIMER = "pp.hera.dal.fetch";
    public static final String FETCH_TIMER_DESCRIPTION = "This metric records the total duration of a FETCH transaction of a certain SQL. It can also describe the max, average, 50th, 90th and 99th percentile of all FETCH transaction durations.";
    public static final String EXEC_SUCCESS_COUNT = "pp.hera.dal.exec.success.count";
    public static final String EXEC_SUCCESS_COUNT_DESCRIPTION = "This metric records the total count of all successful EXEC transactions of a certain SQL.";
    public static final String FETCH_SUCCESS_COUNT = "pp.hera.dal.fetch.success.count";
    public static final String FETCH_SUCCESS_COUNT_DESCRIPTION = "This metric records the total count of all successful FETCH transactions of a certain SQL.";
    public static final String EXEC_FAIL_COUNT = "pp.hera.dal.exec.failure.count";
    public static final String EXEC_FAIL_COUNT_DESCRIPTION = "This metric records the total count of all failed EXEC transactions of a certain SQL.";
    public static final String FETCH_FAIL_COUNT = "pp.hera.dal.fetch.failure.count";
    public static final String FETCH_FAIL_COUNT_DESCRIPTION = "This metric records the total count of all failed FETCH transactions of a certain SQL.";
}
