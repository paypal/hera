package com.paypal.hera.util;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.jdbc.Util;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HeraStatementCacheTest {

    static final Logger LOGGER = LoggerFactory.getLogger(HeraStatementCacheTest.class);

    private static Connection dbConn;

    private static HeraClientConfigHolder.E_DATASOURCE_TYPE datasource = HeraClientConfigHolder.E_DATASOURCE_TYPE.HERA;
    private static String host;

    private static String query;

    @BeforeClass
    public static void setUp() throws Exception {
        Util.makeAndStartHeraMux(null);
        host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
        HeraClientConfigHolder.clear();
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        props.setProperty("hera.query.query1.readTimeout", "1200");
        props.setProperty("hera.query.query2.readTimeout", "798");
        props.setProperty("hera.query.queryXreadTimeout.readTimeout", "1234");
        props.setProperty("hera.query.HELLO.readTimeout", "abcd");
        props.setProperty("hera.query.MyMap.SELECT1.readTimeout", "987");
        dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);
        Statement st = dbConn.createStatement();

        System.out.println(((HeraConnection) dbConn).getHeraClient().getSOTimeout());
        try {
            query = "SELECT HOST_NAME fROM v$instance";
            st.executeQuery(query);
            LOGGER.debug("Testing with Oracle");
            datasource = HeraClientConfigHolder.E_DATASOURCE_TYPE.ORACLE;
        } catch (SQLException ex) {
            query = "Select now()";
            LOGGER.debug("Testing with MySQL");
            datasource = HeraClientConfigHolder.E_DATASOURCE_TYPE.MySQL;
        } finally {
            st.close();
        }
    }

    private int runInternal(String sql) throws SQLException {
        try (Statement st = dbConn.createStatement()) {
            st.executeQuery(sql);
            return ((HeraConnection) dbConn).getHeraClient().getSOTimeout();
        } catch (SocketException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                ((HeraConnection) dbConn).getHeraClient().setSOTimeout(HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS);
            }catch (SocketException e) {}
        }
    }



    @Test
    public void testQueryTimeoutMs() throws SQLException {
        List<TestInputAndExpectation> inputAndExpectations = new ArrayList<>();
        inputAndExpectations.add(new TestInputAndExpectation("/*some other Comment*/" + query + "/* query1 */",
                1200));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/* query11 */",
                HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/* test query1 */",
                HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/* query2*/",
                798));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/*queryXreadTimeout */",
                1234));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/*HELLO*/",
                HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/*MyMap.SELECT1*/",
                987));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/*MyMap.SELECT1**/",
                HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS));
        inputAndExpectations.add(new TestInputAndExpectation(query + "/**MyMap.SELECT1*/",
                HeraClientConfigHolder.DEFAULT_RESPONSE_TIMEOUT_MS));

        for(TestInputAndExpectation inp : inputAndExpectations) {
            Assert.assertEquals("Failed for: " + inp.getSqlInput(),
                    inp.getExpectedTimeoutValue(), runInternal(inp.getSqlInput()));
            System.out.println("Ran SQL: " + inp.getSqlInput() + " Configured Timeout: " + inp.getExpectedTimeoutValue());
        }
    }

    @Test
    public void testCacheDisablePerQuery() throws SQLException {
        HeraConnection connection = (HeraConnection)dbConn;
        HeraStatementsCache.StatementCacheEntry entry = connection.getStatementCache().getEntry(
                query, connection.enableEscape(), connection.shardingEnabled(),
                connection.paramNameBindingEnabled(), datasource);
        Assert.assertTrue(entry.isSqlEligibleForCache());
        String newQuery = "/*DisableStmtCache*/" + query;
        entry = connection.getStatementCache().getEntry(
                newQuery, connection.enableEscape(), connection.shardingEnabled(),
                connection.paramNameBindingEnabled(), datasource);
        Assert.assertFalse(entry.isSqlEligibleForCache());
    }
}

class TestInputAndExpectation {
    private String sqlInput;
    private int expectedTimeoutValue;

    public String getSqlInput() {
        return sqlInput;
    }

    public int getExpectedTimeoutValue() {
        return expectedTimeoutValue;
    }
    TestInputAndExpectation(String sqlInput, int expectedTimeoutValue) {
        this.expectedTimeoutValue = expectedTimeoutValue;
        this.sqlInput = sqlInput;
    }
}
