package com.paypal.hera.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.client.HeraClient;
import com.paypal.hera.client.HeraClientFactory;
import com.paypal.hera.client.HeraClientImpl;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.ex.HeraInternalErrorException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.util.MurmurHash3;
import com.paypal.hera.util.NetStringObj;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraJdbcUtil;

/**
 * 
 * see ClientTest.java for tables setup
 *
 */

public class BatchTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	private static final String sID_START = 	"111777";
	private static final Integer iID_START = 	111777;
	private static final String sINT_VAL1 = "777333";
	private static final Integer iINT_VAL1 = 777333;
	private static final String sINT_VAL2 = "777334";
	private static final Integer iINT_VAL2 = 777334;
	private static final Integer iINT_VAL3 = 777335;
	
	private Connection dbConn;
	private String host;
	private String table;
	private boolean isMySQL;
	
	private int moveRS(ResultSet _rs, int _rows) throws SQLException {
		int moved = 0;
		if (_rows == 0)
			_rows = Integer.MAX_VALUE;
		for (int i = 0; i < _rows; i++) {
			if (!_rs.next())
				break;
			moved++;
		}
		return moved;
	}
	
	void cleanTable(Statement st, String startId, int rows, boolean commit) throws SQLException {
		st.executeUpdate("delete from " + table + " where id >= " + startId + " and id < " + (Integer.parseInt(startId) + rows));
		if (commit)
			dbConn.commit();
	}

	@Before
	public void setUp() throws Exception {
		host = System.getProperty("SERVER_URL", "1:127.0.0.1:11111"); 
		table = System.getProperty("TABLE_NAME", "jdbc_hera_test"); 
		HeraClientConfigHolder.clear();
		Properties props = new Properties();
		props.setProperty("hera.enable.batch", "true");
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
	}

	@After
	public void cleanUp() throws SQLException {
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, true);
		dbConn.close();
	}
	

	@Ignore @Test
	public void test_batches_simple() throws IOException, SQLException{
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setString(2, "row 1");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setString(2, "row 2");
		pst_insert.addBatch();
		
		int[] ret = pst_insert.executeBatch();
		Assert.assertTrue("Results array for batch has 2 elements", ret.length == 2);
		Assert.assertTrue("First query in batch was fine", ret[0] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Second query in batch was fine", ret[1] == PreparedStatement.SUCCESS_NO_INFO);
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	

	@Ignore @Test
	public void test_batches_happy() throws IOException, SQLException{
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val, str_val) values (? , ?, ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setInt(2, 1);
		pst_insert.setString(3, "row 1");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setInt(2, 1);
		pst_insert.setString(3, "row 2");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 2);
		pst_insert.setInt(2, 2);
		pst_insert.setString(3, "row 3");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 3);
		pst_insert.setInt(2, 2);
		pst_insert.setString(3, "row 4");
		pst_insert.addBatch();

		int[] ret = pst_insert.executeBatch();
		Assert.assertTrue("Results array for batch has 4 elements", ret.length == 4);
		Assert.assertTrue("First query in batch was fine", ret[0] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Second query in batch was fine", ret[1] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Third query in batch was fine", ret[2] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Forth query in batch was fine", ret[3] == PreparedStatement.SUCCESS_NO_INFO);

		PreparedStatement pst_update = dbConn.prepareStatement("update " + table + " set int_val = ? where int_val = ?");
		pst_update.setInt(1, 5);
		pst_update.setInt(2, 1);
		pst_update.addBatch();
		pst_update.setInt(1, 5);
		pst_update.setInt(2, 2);
		pst_update.addBatch();
		ret = pst_update.executeBatch();
		Assert.assertTrue("Batched updates were fine", ((ret.length == 2) && (ret[0] == PreparedStatement.SUCCESS_NO_INFO) && 
				(ret[1] == PreparedStatement.SUCCESS_NO_INFO)));

		ResultSet rs = st.executeQuery("select id from " + table + " where int_val=5");
		int rows = 0;
		while (rs.next())
			rows++;
		Assert.assertTrue("Three rows", rows == 4);
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}

	@Ignore @Test
	public void test_batches_failing() throws IOException, SQLException{
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setString(2, "row 1");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setString(2, "row 2");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setString(2, "row 2 bis");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 4);
		pst_insert.setString(2, "row 3");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 4);
		pst_insert.setString(2, "row 3 bis");
		pst_insert.addBatch();
		pst_insert.setInt(1, iID_START + 5);
		pst_insert.setString(2, "row 4");
		pst_insert.addBatch();
		
		try {
			pst_insert.executeBatch();
			Assert.fail("Batch should throw because of constraint violation");
		} catch ( BatchUpdateException ex) {
			int[] ret = ex.getUpdateCounts();
			Assert.assertTrue("Only the third query failed", ((ret.length == 6) 
					&& (ret[0] == PreparedStatement.SUCCESS_NO_INFO)
					&& (ret[1] == PreparedStatement.SUCCESS_NO_INFO)
					&& (ret[2] == PreparedStatement.EXECUTE_FAILED)
					&& (ret[3] == PreparedStatement.SUCCESS_NO_INFO)
					&& (ret[4] == PreparedStatement.EXECUTE_FAILED)
					&& (ret[5] == PreparedStatement.SUCCESS_NO_INFO)
					));			
			dbConn.commit();			
		}
		
		PreparedStatement pst_upd_fail = dbConn.prepareStatement("update " + table + " set id = id+1 where id <= ?");
		pst_upd_fail.setInt(1, iID_START + 3);
		pst_upd_fail.addBatch();
		pst_upd_fail.setInt(1, iID_START + 3);
		pst_upd_fail.addBatch();
		pst_upd_fail.setInt(1, iID_START + 3);
		pst_upd_fail.addBatch();
		try {
			pst_upd_fail.executeBatch();
			Assert.fail("Batch updates should throw because of constraint violation");
		} catch ( BatchUpdateException ex) {
			int[] ret = ex.getUpdateCounts();
			Assert.assertTrue("Only the third query failed", ((ret.length == 3) 
					&& (ret[0] == PreparedStatement.SUCCESS_NO_INFO)
					&& (ret[1] == PreparedStatement.SUCCESS_NO_INFO)
					&& (ret[2] == PreparedStatement.EXECUTE_FAILED)
					));			
			dbConn.commit();
		}
		
		cleanTable(st, sID_START, 20, true);
	}	

	@Ignore @Test
	public void test_batches_1row() throws IOException, SQLException{
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setString(2, "row 1");
		pst_insert.addBatch();
		
		int[] ret = pst_insert.executeBatch();
		Assert.assertTrue("Batch of one row worked", ((ret.length == 1) && (ret[0] == PreparedStatement.SUCCESS_NO_INFO)));		
		try {
			pst_insert.setInt(1, iID_START);
			pst_insert.setString(2, "row 1");
			pst_insert.addBatch();
			ret = pst_insert.executeBatch();
			Assert.fail("Batch should throw because of constraint violation");
		} catch ( BatchUpdateException ex) {
			ret = ex.getUpdateCounts(); 
			Assert.assertTrue("The query failed", ((ret.length == 1) && (ret[0] == PreparedStatement.EXECUTE_FAILED)));			
			dbConn.commit();			
		}

		cleanTable(st, sID_START, 20, true);
	}
	
	@Ignore @Test
	public void test_batches_datetime_binarystr() throws IOException, SQLException{
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val, timestamp_val, timestamp_tz_val) values (? , ?, ?, ?)");
		long now = System.currentTimeMillis();
		pst_insert.setInt(1, iID_START);
		String str = " abc\0xyz  ";
		pst_insert.setString(2, str);
		Timestamp tmst_1 = new Timestamp(now);
		Timestamp tmst_tz_1 = new Timestamp(now);
		Calendar tz = new GregorianCalendar(TimeZone.getTimeZone("GMT-6:00"));
		pst_insert.setTimestamp(3, tmst_1);
		pst_insert.setTimestamp(4, tmst_tz_1, tz);
		pst_insert.addBatch();
		String str2 = "row2\0";
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setString(2, str2);
		Timestamp tmst_2 = new Timestamp(now + 1);
		Timestamp tmst_tz_2 = new Timestamp(now + 2);
		pst_insert.setTimestamp(3, tmst_2);
		pst_insert.setTimestamp(4, tmst_tz_2, tz);
		pst_insert.addBatch();

		int[] ret = pst_insert.executeBatch();
		dbConn.commit();
		Assert.assertTrue("Results array for batch has 2 elements", ret.length == 2);
		Assert.assertTrue("First query in batch was fine", ret[0] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Second query in batch was fine", ret[1] == PreparedStatement.SUCCESS_NO_INFO);

		ResultSet rs = st.executeQuery("select id, str_val, timestamp_val, timestamp_tz_val from " + table + " where id>=" + iID_START + " and id <=" + (iID_START + 1) + " order by id asc");
		Assert.assertTrue("1st row", rs.next());
		Assert.assertTrue("str val correct", str.equals(rs.getString(2)));
		Assert.assertTrue("timestamp correct", rs.getTimestamp(3).equals(tmst_1));
		Assert.assertTrue("timestamp TZ correct", rs.getTimestamp(4, tz).equals(tmst_tz_1));
		Assert.assertTrue("timestamp TZ offset correct", rs.getTimestamp(4, tz).getTimezoneOffset() == tmst_tz_1.getTimezoneOffset());
		Assert.assertTrue("2nd row", rs.next());
		Assert.assertTrue("str2 val correct", str2.equals(rs.getString(2)));
		Assert.assertTrue("timestamp correct", rs.getTimestamp(3).equals(tmst_2));
		Assert.assertTrue("timestamp TZ correct", rs.getTimestamp(4, tz).equals(tmst_tz_2));
		Assert.assertTrue("timestamp TZ offset correct", rs.getTimestamp(4, tz).getTimezoneOffset() == tmst_tz_2.getTimezoneOffset());
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}
	
	@Ignore @Test
    public void test_batches_blob_4k() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
	    cleanTable(st, sID_START, 20, false);
	    dbConn.commit();
	    
	    PreparedStatement pst_insert = dbConn.prepareStatement("/* Hera 2 */insert into " + table + " (id, blob_val, int_val) values (? , ?, ?)");
	    pst_insert.setInt(1, iID_START);
	    byte[] blobBytes = new byte[9001];
	    for (int i = 0; i < blobBytes.length; i++) {
	    	blobBytes[i] = (byte)(i % 256);
	    }
	    Blob blob = dbConn.createBlob();
	    blob.setBytes(1, blobBytes);
	    pst_insert.setBlob(2, blob);
	    pst_insert.setInt(3, 123);
	    pst_insert.addBatch();
	    pst_insert.setInt(1, iID_START + 1);
	    for (int i = 0; i < blobBytes.length; i++) {
	    	blobBytes[i] = (byte)((i + 20) % 256);
	    }
	    blob.setBytes(1, blobBytes);
	    pst_insert.setBlob(2, blob);
	    pst_insert.setInt(3, 444555);
	    pst_insert.addBatch();
	    
	    try {
	    int[] ret = pst_insert.executeBatch();
	        Assert.assertTrue("Results array for batch has 2 elements", ret.length == 2);
	        Assert.assertTrue("First query in batch was fine", ret[0] == PreparedStatement.SUCCESS_NO_INFO);
	        Assert.assertTrue("Second query in batch was fine", ret[1] == PreparedStatement.SUCCESS_NO_INFO);
	        dbConn.commit();
	        ResultSet rs = st.executeQuery("select id, blob_val, int_val from " + table + " where id in (" + iID_START + "," + (iID_START + 1) + ") order by id");
	        Assert.assertTrue("First row", rs.next());
	        Assert.assertTrue("id", rs.getInt(1) == iID_START);
	        byte[] bytes = rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length());
	        for (int i = 0; i < blobBytes.length; i++) {
	        	if (bytes[i] != (byte)(i % 256))
	        		Assert.fail("bytes[" + i + "]: " + bytes[i]);
	        }
	        Assert.assertTrue("int_val", rs.getInt(3) == 123);
	        
	        Assert.assertTrue("Second row", rs.next());
	        Assert.assertTrue("id", rs.getInt(1) == iID_START + 1);
	        bytes = rs.getBlob(2).getBytes(1, (int) rs.getBlob(2).length());
	        for (int i = 0; i < blobBytes.length; i++) {
	        	if (bytes[i] != (byte)((i + 20) % 256))
	        		Assert.fail("bytes[" + i + "]: " + bytes[i]);
	        }
	        Assert.assertTrue("int_val", rs.getInt(3) == 444555);
	    } catch (BatchUpdateException ex) {
	    	Assert.fail("batch ex");                    	
	    }
	    
	    cleanTable(st, sID_START, 20, false);
	    dbConn.commit();
    }

	@Ignore @Test
    public void test_batch_sharding() throws IOException, SQLException{
		if (isMySQL)
			return;
		Statement st = dbConn.createStatement();
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		int shardVal1 = iID_START;
		int shardVal2 = iID_START + 1;
		pst_insert.setInt(1, shardVal1);
		pst_insert.setString(2, "row 1");
		pst_insert.addBatch();
		pst_insert.setInt(1, shardVal2);
		pst_insert.setString(2, "row 2");
		pst_insert.addBatch();

		int[] ret = pst_insert.executeBatch();
		Assert.assertTrue("Results array for batch has 2 elements", ret.length == 2);
		Assert.assertTrue("First query in batch was fine", ret[0] == PreparedStatement.SUCCESS_NO_INFO);
		Assert.assertTrue("Second query in batch was fine", ret[1] == PreparedStatement.SUCCESS_NO_INFO);
		ResultSet rs = st.executeQuery("select scuttle_id from " + table + " where id = " + Integer.toString(shardVal1));
		Assert.assertTrue("Scuttle ID for first query is correct", get_scuttle_id(Integer.toString(shardVal1)) == rs.getInt(1));
    }

	public int get_scuttle_id(String input) throws IOException, SQLException{
		byte[] data = input.getBytes();
		return HeraJdbcUtil.getScuttleID(data);
	}
}
