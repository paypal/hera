package com.paypal.hera.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.client.HeraClientImpl;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.util.MurmurHash3;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraJdbcUtil;

/**
 * 
 * see ClientTest.java for tables setup
 *
 */

public class MySqlLastInsertIdTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	
	private Connection dbConn;
	private String host;
	private boolean isMySQL;
	
	@Before
	public void setUp() throws Exception {
		host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111"); 
		HeraClientConfigHolder.clear();
		Properties props = new Properties();
		props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
		props.setProperty(HeraClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
		props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
		props.setProperty(HeraClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
		Class.forName("com.paypal.hera.jdbc.HeraDriver");
		dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

		// determine database server
		HeraConnection hera = (HeraConnection)dbConn;
		hera.setShardHint("shardid", "0");
		Statement st = dbConn.createStatement();
		try {
			st.executeQuery("SELECT HOST_NAME fROM v$instance");
			isMySQL = false;
			LOGGER.debug("Testing with Oracle");
		} catch (SQLException ex) {
			isMySQL = true;
			LOGGER.debug("Testing with MySQL");
		}
		hera.resetShardHints();
	}

	@After
	public void cleanUp() throws SQLException {
		dbConn.close();
	}
	
	@Test
	public void test_mysql_last_insert_id() throws IOException, SQLException{
		if (!isMySQL) 
			return;

		try {
			PreparedStatement pst = dbConn.prepareStatement("create table test_mysql_last_insert_id ( id int, note varchar(55), autoI int not null auto_increment, primary key (autoI) )");
			pst.executeUpdate();
		} catch (Throwable t) {
			// ignore errors in setup
		}
		PreparedStatement pst0 = dbConn.prepareStatement("delete from test_mysql_last_insert_id ");
		pst0.executeUpdate();
		// setup done

		PreparedStatement pst2 = dbConn.prepareStatement("insert into test_mysql_last_insert_id ( id , note ) values ( ?, ? )");
		pst2.setInt(1, 11);
		pst2.setString(2, "eleven");
		pst2.executeUpdate();
		ResultSet rs = pst2.getGeneratedKeys();
		long id = -1;
		try {
			id = rs.getLong(0);
		} catch (Throwable t) {
			Assert.assertTrue("oops no generated key", false);
		}
		
		pst2.setInt(1, 12);
		pst2.setString(2, "twelve");
		pst2.executeUpdate();
		ResultSet rs3 = pst2.getGeneratedKeys();
		long id3 = -1;
		try {
			id3 = rs3.getLong(0);
		} catch (Throwable t) {
			Assert.assertTrue("oops no last insert id", false);
		}
		System.out.println("last insert id are "+id+" "+id3);
		Assert.assertTrue("diff rows inserted, expect diff ids", id3 != id);

		// some testing to see that the connection and protocol are good

		PreparedStatement ps;
		ps = dbConn.prepareStatement("select id from test_mysql_last_insert_id");
		ps.executeQuery();
		
		ps = dbConn.prepareStatement("update test_mysql_last_insert_id set note='Eleven' where id=11");
		ps.executeUpdate();

		ps = dbConn.prepareStatement("delete from test_mysql_last_insert_id where id=12");
		ps.executeUpdate();
	}
}
