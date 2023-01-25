package com.paypal.hera.jdbc;


import com.paypal.hera.micrometer.MicrometerTest;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.IOException;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        MySqlLastInsertIdTest.class,
        MySqlTxnTest.class,
        MicrometerTest.class
})


public class MySqlTestSuite {
    @AfterClass
    public static void cleanup() throws IOException, InterruptedException {
        Runtime.getRuntime().exec("docker stop mysql55").waitFor();
        Runtime.getRuntime().exec("docker rm mysql55").waitFor();
        Runtime.getRuntime().exec("killall -ILL mux mysqlworker").waitFor();
    }
}
