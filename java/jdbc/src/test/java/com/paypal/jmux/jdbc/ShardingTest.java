package com.paypal.jmux.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.paypal.jmux.ex.OccClientException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.jmux.client.OccClientImpl;
import com.paypal.jmux.conf.OCCClientConfigHolder;
import com.paypal.jmux.util.MurmurHash3;
import com.paypal.jmux.util.OccJdbcConverter;
import com.paypal.jmux.util.OccJdbcUtil;

/**
 * 
 * see ClientTest.java for tables setup
 *
 */

public class ShardingTest {
	static final Logger LOGGER = LoggerFactory.getLogger(OccClientImpl.class);
	private static final String sID_START = 	"111777";
	private static final Integer iID_START = 	111777;
	
	private Connection dbConn;
	private String host;
	private String table;
	private boolean isMySQL;
	
	void cleanTable(Statement st, String startId, int rows, boolean commit) throws SQLException {
		st.executeUpdate("delete from " + table + " where id >= " + startId + " and id < " + (Integer.parseInt(startId) + rows));
		if (commit)
			dbConn.commit();
	}

	@Before
	public void setUp() throws Exception {
		host = System.getProperty("SERVER_URL", "1:127.0.0.1:10101"); 
		table = System.getProperty("TABLE_NAME", "jdbc_mux_test"); 
		OCCClientConfigHolder.clear();
		Properties props = new Properties();
		props.setProperty(OCCClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
		props.setProperty(OCCClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY, "true");
		props.setProperty(OCCClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "true");
		props.setProperty(OCCClientConfigHolder.ENABLE_SHARDING_PROPERTY, "true");
		Class.forName("com.paypal.jmux.jdbc.OccDriver");
		dbConn = DriverManager.getConnection("jdbc:occ:" + host, props);

		// determine database server
		OccConnection occ = (OccConnection)dbConn;
		occ.setShardHint("shardid", "0");
		Statement st = dbConn.createStatement();
		try {
			st.executeQuery("SELECT HOST_NAME fROM v$instance");
			isMySQL = false;
			LOGGER.debug("Testing with Oracle");
		} catch (SQLException ex) {
			isMySQL = true;
			LOGGER.debug("Testing with MySQL");
		}
		occ.resetShardHints();
	}

	@After
	public void cleanUp() throws SQLException {
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, true);
		dbConn.close();
	}
	
	@Test
	public void test_sharding() throws IOException, SQLException{
		if (isMySQL) 
			return;
		byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
		int hash = MurmurHash3.murmurhash3_x86_32(data, 0, data.length, 0x183d1db4);
		Assert.assertTrue("Hash ", hash == 1696781095);
		data = "The".getBytes();
		hash = MurmurHash3.murmurhash3_x86_32(data, 0, data.length, 0x183d1db4);
		Assert.assertTrue("Hash ", hash == -1032052823);

		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val) "
				+ "values (?, ?) /* OCCSK=id(1),ScuttleId(2) */");
		pst.setInt(1, iID_START);
		pst.setInt(2, 0);
		pst.executeUpdate();
		pst.setInt(1, iID_START + 1);
		pst.setInt(2, 0);
		pst.executeUpdate();
		dbConn.commit();
		
		pst = dbConn.prepareStatement("select /*CAL comment*/ id, int_val from " + table + " "
				+ "where id in (?,?,?) and int_val in (?,?,?) "
				+ "/* OCCSK=id(1,2,3),ScuttleId(4,5,6) */ order by id");
		pst.setInt(1, iID_START);
		pst.setInt(2, iID_START + 1);
		pst.setInt(3, iID_START + 2);
		if (!((OccConnection)dbConn).shardingEnabled()) {
			pst.setInt(4, 0);
			pst.setInt(5, 0);
			pst.setInt(6, 0);
		}

		ResultSet rs = pst.executeQuery();
		Assert.assertTrue("Got 1 row", rs.next());
		Assert.assertTrue("Got result id", rs.getInt(1) == iID_START);
		if (((OccConnection)dbConn).shardingEnabled()) 
			Assert.assertTrue("Got result scuttle id", rs.getInt(2) == OccJdbcUtil.getScuttleID(OccJdbcConverter.int2occ(iID_START)));
		Assert.assertTrue("Got 2 rows", rs.next());
		Assert.assertTrue("Got result id + 1", rs.getInt(1) == iID_START + 1);
		if (((OccConnection)dbConn).shardingEnabled()) 
			Assert.assertTrue("Got result scuttle id + 1", rs.getInt(2) == OccJdbcUtil.getScuttleID(OccJdbcConverter.int2occ(iID_START + 1)));
		Assert.assertTrue("No more rows", !rs.next());
	}
	
	@Test
	public void test_scuttle_id() throws IOException, SQLException{
		if (isMySQL) 
			return;
		byte[] data = "1703900906402232986".getBytes();
		int sid = OccJdbcUtil.getScuttleID(data);
		Assert.assertTrue("scuttle_id ", sid == 470);
	}
	
	@Test
	public void test_sharding_api() throws IOException, SQLException{
		if (isMySQL) 
			return;
		OccConnection occ = (OccConnection)dbConn;
		int shards = occ.getShardCount();
		System.out.println("Shard #: " + shards);
		occ.setShardHint("shardid", "0");
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val) "
				+ "values (?, ?) /* OCCSK=id(1),ScuttleId(2) */");
		pst.setInt(1, iID_START);
		pst.setInt(2, 0);
		try {
			pst.executeUpdate();
			if (occ.shardingEnabled())
				Assert.fail("Should have thrown");
		} catch (OccClientException ex) {
			Assert.assertTrue("Exception expected: " + ex.getMessage(), true);
		}
		
		occ.resetShardHints();
		if (occ.shardingEnabled())
			pst.executeUpdate();
		occ.rollback();
		Assert.assertTrue("Update works after shard hint reset", true);
	}
}
