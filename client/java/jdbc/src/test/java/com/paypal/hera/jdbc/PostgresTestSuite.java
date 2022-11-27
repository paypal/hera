package com.paypal.hera.jdbc;


import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        PgCurdTest.class,
        PostgresTxnTest.class,
})

public class PostgresTestSuite {
    @AfterClass
    public static void cleanup() throws IOException, InterruptedException {
        Runtime.getRuntime().exec("docker stop postgres55").waitFor();
        Runtime.getRuntime().exec("docker rm postgres55").waitFor();
        Runtime.getRuntime().exec("killall -ILL mux postgresworker").waitFor();
    }
}
