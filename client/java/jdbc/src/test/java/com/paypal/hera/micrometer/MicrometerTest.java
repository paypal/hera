package com.paypal.hera.micrometer;

import com.paypal.dal.occmockclient.OCCMockAction;
import com.paypal.dal.occmockclient.OCCMockHelper;
import com.paypal.hera.client.HeraClientImpl;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.jdbc.Util;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import static com.paypal.hera.constants.MicrometerConsts.*;

public class MicrometerTest {
    private static Connection dbConn;
    private static String host;
    private static String table;
    private static boolean isMySQL;
    static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
    private static final String sID_START = 	"111777";
    private static final Integer iID_START = 	111777;
    private static final String sINT_VAL1 = "777333";
    private static final Integer iINT_VAL1 = 777333;
    private static final String sINT_VAL2 = "777334";
    private static final Integer iINT_VAL2 = 777334;
    private static final Integer iINT_VAL3 = 777335;
    private static final Integer STEP_MS = 60000;


    private static MicrometerTestSetup setup = MicrometerTestSetup.getInstance();
    private static MockSignalFxMeterRegistry registry = setup.getRegistry();
    void cleanTable(Statement st, String startId, int rows, boolean commit) throws SQLException {
        st.executeUpdate("delete from " + table + " where id >= " + startId + " and id < " + (Integer.parseInt(startId) + rows));
        if (commit)
            dbConn.commit();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Util.makeAndStartHeraMux(null);
        host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111");
        table = System.getProperty("TABLE_NAME", "jdbc_hera_test");
        HeraClientConfigHolder.clear();
        Properties props = new Properties();
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
        dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

        // determine database server
        Statement st = dbConn.createStatement();
        try {
            st.executeQuery("SELECT HOST_NAME fROM v$instance");
            isMySQL = false;
            LOGGER.debug("Testing with Oracle");
        } catch (SQLException ex) {
            isMySQL = true;
            LOGGER.debug("Testing with MySQL");
        }

        if (isMySQL) {
            LOGGER.info("Re-create table: " + table);
            try {
                st.executeUpdate("drop table " + table + "");
                dbConn.commit();
            } catch (SQLException ex) {
                LOGGER.debug("table doesn't exists: " + ex.getMessage());
            }
            try {
                st.executeUpdate("CREATE TABLE `" + table + "` (\n" +
                        "  `ID` int(11) NOT NULL,\n" +
                        "  `INT_VAL` int(11) DEFAULT NULL,\n" +
                        "  `STR_VAL` varchar(256) DEFAULT NULL,\n" +
                        "  `CHAR_VAL` int(2) DEFAULT NULL,\n" +
                        "  `FLOAT_VAL` float(38,10) DEFAULT NULL,\n" +
                        "  `RAW_VAL` tinyblob,\n" +
                        "  `blob_val` blob,\n" +
                        "  `clob_val` text,\n" +
                        "  `date_val` date DEFAULT NULL,\n" +
                        "  `time_val` timestamp NULL DEFAULT NULL,\n" +
                        "  `timestamp_val` timestamp NULL DEFAULT NULL,\n" +
                        "  `timestamp_tz_val` timestamp NULL DEFAULT NULL,\n" +
                        "  PRIMARY KEY (`ID`)\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
                dbConn.commit();
            } catch(SQLException ex) {
                LOGGER.error("Caught ex during setup (create table):" + ex.getMessage());
                throw ex;
            }
        } else {
            try {
                st.executeUpdate("drop table " + table + "");
                dbConn.commit();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 942) {
                    LOGGER.debug("table doesn't exists");
                } else {
                    LOGGER.error("Caught ex during setup (drop table):" + ex.getMessage());
                    throw ex;
                }
            }
            try {
                st.executeUpdate("	create table " + table + " (" +
                        "	ID                                      NUMBER primary key ," +
                        "	INT_VAL                                 NUMBER," +
                        "	STR_VAL                                 VARCHAR2(256)," +
                        "	CHAR_VAL                                NUMBER(2)," +
                        "	FLOAT_VAL                               NUMBER(38,10)," +
                        "	RAW_VAL                                 RAW(1000)," +
                        "	blob_val                                BLOB," +
                        "	clob_val                                CLOB," +
                        "	date_val								DATE," +
                        "	time_val								DATE," +
                        "	timestamp_val							TIMESTAMP," +
                        "	timestamp_tz_val 						timestamp with time zone" +
                        "	)");
                dbConn.commit();
            } catch(SQLException ex) {
                LOGGER.error("Caught ex during setup (create table):" + ex.getMessage());
                throw ex;
            }
            try {
                st.executeUpdate("CREATE OR REPLACE PROCEDURE sp_test1(" +
                        "		   p_id IN Number," +
                        "		   o_id OUT Number," +
                        "		   o_str_val OUT  VARCHAR2," +
                        "		   o_float_val OUT number)" +
                        "	IS" +
                        "	BEGIN" +
                        "	 " +
                        "	  SELECT int_val , str_val, float_val" +
                        "	  INTO o_id, o_str_val,  o_float_val " +
                        "	  FROM  " + table + " WHERE id = p_id;" +
                        "	 " +
                        "	END;");
                dbConn.commit();
            } catch(SQLException ex) {
                LOGGER.error("Caught ex during setup (create SP):" + ex.getMessage());
                throw ex;
            }
            LOGGER.info("Setup OK");
        }
    }

    @After
    public void cleanUp() throws SQLException {
        if (!dbConn.isClosed()) {
            Statement st = dbConn.createStatement();
            cleanTable(st, sID_START, 20, true);
        }
    }

    @AfterClass
    public static void cleanUpAll() throws SQLException {
        dbConn.close();
        LOGGER.info("Done");
    }

    @Test
    public void testCounts() throws IOException, SQLException, InterruptedException {
        Statement st = dbConn.createStatement();
        cleanTable(st, sID_START, 20, false);
        final int ROWS = 10;
        for (int i = 0; i < ROWS; i++)
            Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);
        Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)") == 1);
        dbConn.commit();

        PreparedStatement pst;

        pst = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=? and str_val=?");
        pst.setInt(1, iINT_VAL1);
        pst.setString(2, "abcd");
        pst.setFetchSize(0);
        pst.executeQuery();

        pst.setInt(1, iINT_VAL3);
        pst.executeQuery();

        pst.setInt(1, iINT_VAL2);
        pst.executeQuery();

        pst.clearParameters();

        try{
            OCCMockHelper.addMock("fetch_fail", OCCMockAction.TIMEOUT_ON_FETCH);
            pst = dbConn.prepareStatement("select /* fetch_fail */ int_val, str_val, float_val from " + table + " where int_val=? and str_val=?");
            pst.setInt(1, iINT_VAL1);
            pst.setString(2, "abcd");
            pst.executeQuery();
        }
        finally{
            OCCMockHelper.removeMock("fetch_fail");
        }

        cleanTable(dbConn.createStatement(), sID_START, 20, true);

        Thread.sleep(STEP_MS);

        Map<String, ArrayList<MeterInfoTest>> publishedData = registry.getMeterInfoMap();
        if(!publishedData.isEmpty()){
            ArrayList<MeterInfoTest> execSuccess = publishedData.get(EXEC_SUCCESS_COUNT);
            ArrayList<MeterInfoTest> execFail = publishedData.get(EXEC_FAIL_COUNT);
            ArrayList<MeterInfoTest> fetchSuccess = publishedData.get(FETCH_SUCCESS_COUNT);

            if(execSuccess == null){
                Assert.fail("No data sent");
            }

            if(execFail == null){
                Assert.fail("No data sent");
            }

            if(fetchSuccess == null){
                Assert.fail("No data sent");
            }

            int execSum = 0;
            for (MeterInfoTest info : execSuccess) {
                int val = ((Double) info.getValue()).intValue();
                execSum += val;
                Assert.assertEquals("unknown", info.getHost());
                Assert.assertEquals("0", info.getSqlHash());
            }
            Assert.assertEquals(19, execSum);

            int execFailSum = 0;
            for (MeterInfoTest info : execFail) {
                int val = ((Double) info.getValue()).intValue();
                execFailSum += val;
                Assert.assertEquals("unknown", info.getHost());
                Assert.assertEquals("0", info.getSqlHash());
            }
            Assert.assertEquals(1, execFailSum);

            int fetchSum = 0;
            for (MeterInfoTest info : fetchSuccess) {
                int val = ((Double) info.getValue()).intValue();
                fetchSum += val;
                Assert.assertEquals("unknown", info.getHost());
                Assert.assertEquals("0", info.getSqlHash());

            }
            Assert.assertEquals(4, fetchSum);
        }

    }
}
