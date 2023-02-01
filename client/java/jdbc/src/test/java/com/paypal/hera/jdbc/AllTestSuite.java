package com.paypal.hera.jdbc;


import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        MySqlTestSuite.class,
        PostgresTestSuite.class,
})

public class AllTestSuite {
}
