package com.paypal.hera.jdbc;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Test;
import java.sql.DriverManager;
import java.io.IOException;
import oracle.jdbc.driver.OracleDriver;
import java.sql.ResultSetMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;


/**
 * @since: Mar 2025
 * @author: anagopal
 */
public class ClientInfoPoolnameTest {
    
	private final static  Logger LOGGER = LoggerFactory.getLogger(ClientInfoPoolnameTest.class);
	private static Connection  dirDBConn;
	private static  HeraConnection dbConn,dbConn2;
	
	@BeforeClass
	public static void setUp() throws IOException, InterruptedException{
	LOGGER.debug("ClientInfoPoolnameTest :: setUp {}","starting oralce and mux");
        Util.startOracleContainer("oracle-xe-21","heraapp","heraappstg");
	Util.makeAndStartMuxOracleWorker(null);
    }

    @AfterClass
    public static void teardown() throws IOException, InterruptedException {	
	LOGGER.debug("ClientInfoPoolnameTest :: teardown {}","sttopping  oralce and mux");
	Runtime.getRuntime().exec("docker stop oracle-xe-21").waitFor();
	Runtime.getRuntime().exec("docker rm oracle-xe-21").waitFor();
	Runtime.getRuntime().exec("killall -ILL mux oracleworker").waitFor();
    }


   @Test
    public void testClientInfoPoolname() throws IOException, SQLException, InterruptedException {
	LOGGER.debug("ClientInfoPoolnameTest :: testClientInfoPoolname {}","starting test");
        dirDBConn = getDBConnection();

        //check if oracle container is running
        Runtime.getRuntime().exec("docker inspect -f '{{.State.Running}}' oracle-xe-21");

        dbConn = Util.makeDbConn();
        dbConn2 = Util.makeDbConn();

        /**
         * set a pool name for the first connection
         */
        dbConn.getHeraClient().setPoolName("heraapp1");
        dbConn.getHeraClient().sendClientInfo("Poolname", "");
	LOGGER.debug("Poolname set to \'heraapp1\'");
        PreparedStatement st = dbConn.prepareStatement("select * from pool_details");
        ResultSet rs = st.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            System.out.println("ID: " + id + ", Name: " + name);
        }
	
        PreparedStatement stmtVssn = dirDBConn.prepareStatement("select action from v$session where module=\'hera-test\'");
        LOGGER.debug("Query v$session ::","select action from v$session where module=\'hera-test\'");
	ResultSet vsRs = stmtVssn.executeQuery();
	ResultSetMetaData resultSetMetaData = vsRs.getMetaData();
        String action = "";
        while(vsRs.next()){
            action = vsRs.getString("action");
        }
        LOGGER.debug("Action in v$session  :: "+action);
        Assert.assertNotNull(action);
	String [] actions = action.split(":");
        Assert.assertTrue(Arrays.stream(actions).anyMatch(act -> act.equalsIgnoreCase("heraapp1")));
        Assert.assertTrue(Arrays.stream(actions).anyMatch(act -> act.equalsIgnoreCase("1228873317")));

       	/**
         * set different poolname for different connection
         */
        dbConn2.getHeraClient().setPoolName("heraapp2");
        dbConn2.getHeraClient().sendClientInfo("Poolname", "");
	LOGGER.debug("Poolname set to \'heraapp2\'");
        PreparedStatement st1 = dbConn2.prepareStatement("select id,name from pool_details");
        ResultSet rs1 = st1.executeQuery();
	while (rs1.next()) {
            int id = rs1.getInt("id");
            String name = rs.getString("name");
            LOGGER.debug("ID: " + id + ", Name: " + name);
        }
        
	ResultSet vsRs1 = stmtVssn.executeQuery();
        String action1 = "";
        while(vsRs1.next()){
            action1 = vsRs1.getString("action");
        }
        LOGGER.debug("Action in v$session :: "+action1);
        Assert.assertNotNull(action1);
	String [] actions1 = action1.split(":");
        Assert.assertTrue(Arrays.stream(actions1).anyMatch(act -> act.equalsIgnoreCase("heraapp2")));
        Assert.assertTrue(Arrays.stream(actions1).anyMatch(act -> act.equalsIgnoreCase("304854272")));
    }

   private Connection getDBConnection(){
        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521/poolname";
        String username = "heraapp";
        String password = "heraappstg";
	
	LOGGER.debug("Connecting to database directly :: "+jdbcUrl);
        int maxRetries = 10;
        int retryCount = 0;
        boolean success = false;
        while (retryCount < maxRetries && !success) {
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                LOGGER.debug("Connected to Oracle database successfully!");
                success =true;
                return connection;
            } catch (SQLException e) {
                LOGGER.error("Error connecting to Oracle database:",new Exception(e));
            }catch (Exception exc){
                LOGGER.error("Error connecting to Oracle database: ", new Exception(exc));
            }
        }
        return null;
    }
}
