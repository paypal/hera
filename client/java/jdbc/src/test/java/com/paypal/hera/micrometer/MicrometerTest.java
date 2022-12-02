package com.paypal.hera.micrometer;

import com.paypal.dal.occmockclient.OCCMockHelper;
import com.paypal.hera.conf.HeraClientConfigHolder;
import org.junit.*;
import org.junit.rules.TestName;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import static com.paypal.hera.constants.MicrometerConsts.*;

public class MicrometerTest {
    private String host = System.getProperty("occ_conf", "1:localhost:10101"); ;
    private String url = "jdbc:hera:" + host;
    private static Properties props = new Properties();
    private static final Integer iINT_VAL1 = 777333;
    private static final Integer STEP_MS = 60000;
    private static MicrometerTestSetup setup = MicrometerTestSetup.getInstance();
    private static MockSignalFxMeterRegistry registry = setup.getRegistry();
    private String serverIp;



    @Rule
    public TestName testName = new TestName();


    @Before
    public void setUp() throws ClassNotFoundException, InterruptedException {
        serverIp = OCCMockHelper.getServerIp();
        OCCMockHelper.setServerIp("10.57.72.249");
        System.setProperty("keymaker.test.appname","occjdbc-unittest");
        HeraClientConfigHolder.clear();
        Properties props = new Properties();
        props.setProperty("host", "occ-user");
        props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "15000");
        props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
        props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");

    }

    @After
    public void tearDown() {
        OCCMockHelper.setServerIp(serverIp);
        registry.cleanUp();
    }


    @Test
    public void testCount() throws SQLException, InterruptedException {
        Connection dbConn = DriverManager.getConnection(url, props);
        PreparedStatement ps = dbConn.prepareStatement("select int_val, str_val, float_val from jdbc_occ_test where int_val=?");
        ps.setInt(1, iINT_VAL1 + 1);

        //execute the same select query 20 times
        for (int i = 0; i< 10; i++){
            ResultSet rsp = ps.executeQuery();
            Thread.sleep(100);
            rsp.close();
        }
        ps.close();
        dbConn.close();


        OCCMockHelper.setServerIp("MOCK_SERVER");
        OCCMockHelper.addMock("mock_test","0 3:3 2,3:3 0,, NEXT_NEWSTRING 0 3:3 2,9:3 INT_VAL,3:3 2,3:3 0,3:3 0,5:3 129,9:3 STR_VAL,3:3 1,5:3 "
                +"256,3:3 0,3:3 0,, NEXT_NEWSTRING 6", -1, -1);
        OCCMockHelper.disableLogging();
        Connection dbConn2 = DriverManager.getConnection(url, props);
        PreparedStatement ps2 = dbConn2.prepareStatement("select /* mock_test */ int_val, str_val, float_val from jdbc_occ_test where int_val=?");
        ps2.setInt(1, iINT_VAL1 + 1);

        try {
            for (int i = 0; i < 10; i++) {
                ResultSet rsp = ps2.executeQuery();
                rsp.close();
            }
        }
        finally{
            ps2.close();
            dbConn2.close();
            OCCMockHelper.removeMock("mock_test");
        }
        //sleep for the step amount to ensure all the values are published together
        Thread.sleep(STEP_MS);


        //verification
        Map<String, ArrayList<MeterInfoTest>> publishedData = registry.getMeterInfoMap();

        if(!publishedData.isEmpty()){
            ArrayList<MeterInfoTest> execSuccess = publishedData.get(EXEC_SUCCESS_COUNT);
            ArrayList<MeterInfoTest> fetchSuccess = publishedData.get(FETCH_SUCCESS_COUNT);

            if(execSuccess == null){
                Assert.fail("No data sent");
            }

            if(fetchSuccess == null){
                Assert.fail("No data sent");
            }

            //verify that all SELECT query transactions were recorded
            //verify that 2 unique queries ran
            int execSum = 0;
            HashSet<String> execSqlHashes = new HashSet<>();
            for (MeterInfoTest info : execSuccess) {
                int val = ((Double) info.getValue()).intValue();
                execSum += val;
                Assert.assertEquals("occ-user", info.getHost());
                if (val > 0){
                    execSqlHashes.add(info.getSqlHash());
                    Assert.assertEquals(10, val);
                }
            }
            Assert.assertEquals(20, execSum);
            Assert.assertEquals(2, execSqlHashes.size());

            int fetchSum = 0;
            HashSet fetchSqlHashes = new HashSet();
            for (MeterInfoTest info : fetchSuccess) {
                int val = ((Double) info.getValue()).intValue();
                fetchSum += val;
                Assert.assertEquals("occ-user", info.getHost());
                if (val > 0){
                    fetchSqlHashes.add(info.getSqlHash());
                    Assert.assertEquals(10, val);
                }

            }
            Assert.assertEquals(20, fetchSum);
            Assert.assertEquals(2, fetchSqlHashes.size());
        }

    }
}
