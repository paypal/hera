package com.paypal.hera.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
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
import java.sql.SQLFeatureNotSupportedException;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
import com.paypal.hera.jdbc.HeraBlob;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.util.NetStringObj;

public class ClientTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	private static final String sID_START = 	"111777";
	private static final Integer iID_START = 	111777;
	private static final String sINT_VAL1 = "777333";
	private static final Integer iINT_VAL1 = 777333;
	private static final String sINT_VAL2 = "777334";
	private static final Integer iINT_VAL2 = 777334;
	private static final Integer iINT_VAL3 = 777335;
	
	private static Connection dbConn;
	private static String host;
	private static String table;
	private static boolean isMySQL;
	
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
	public void test_basic_DML() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Delete all", st.executeUpdate("delete from " + table + " where id=" + sID_START) == 0);
		final int ROWS = 10;
		for (int i = 0; i < ROWS; i++)
			Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		dbConn.commit();
		Assert.assertTrue("Update " + ROWS + " rows", st.executeUpdate("update " + table + " set str_val='xyz' where int_val=" + sINT_VAL1) == ROWS);
		Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + ", " + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);		
		Assert.assertTrue("Update " + (ROWS + 1) + " rows", st.executeUpdate("update " + table + " set str_val='xyz2' where int_val=" + sINT_VAL1) == ROWS + 1);
		dbConn.rollback();
		Assert.assertTrue("Update " + ROWS + " rows", st.executeUpdate("update " + table + " set str_val='xyz3' where int_val=" + sINT_VAL1) == ROWS);
		cleanTable(st, sID_START, 20, true);
	}
	
	@Test	
	public void test_fetch() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		final int ROWS = 10;
		for (int i = 0; i < ROWS; i++)
			Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		
		ResultSet rs = st.executeQuery("select int_val,str_val from " + table + " where int_val=" + sINT_VAL1);
		ResultSetMetaData meta = rs.getMetaData();
		Assert.assertTrue("Column name id", meta.getColumnName(1).equalsIgnoreCase("int_val"));
		Assert.assertTrue("Column name str_val", meta.getColumnName(2).equalsIgnoreCase("str_val"));

		int[] numbers = {0, 1, 3, ROWS - 1, ROWS, ROWS + 1};
		for (int chunk_size: numbers) {
			st.setFetchSize(chunk_size);
			rs = st.executeQuery("select int_val,str_val from " + table + " where int_val=" + sINT_VAL1);
			int rows = 0;
			while (rs.next())
				rows++;
			Assert.assertTrue("Fetched " + ROWS + " rows", rows == ROWS);
		}
		cleanTable(st, sID_START, 20, true);
	}
	
	@Test	
	public void test_bad_sql() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		try{
			st.executeUpdate("not a SQL query");
			Assert.fail("executeUpdate: Invalid query should throw");
		} catch (SQLException e) {
			Assert.assertTrue("executeUpdate: Invalid query should throw", true);
		}
		
		try{
			st.executeQuery("not a SQL query");
			Assert.fail("executeQuery: Invalid query should throw");
		} catch (SQLException e) {
			Assert.assertTrue("executeQuery: Invalid query should throw", true);
		}
	}	
	
	@Test
	public void test_prepare_st() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		final int ROWS = 10;
		for (int i = 0; i < ROWS; i++)
			Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		dbConn.commit();
		
		PreparedStatement pst;

		// run first with binding just one value
		pst = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=? and str_val=?");
		pst.setInt(1, iINT_VAL1);
		boolean gotExpectedErr = false;
		try {
			pst.executeQuery();
		} catch(SQLException e) {
			gotExpectedErr = true;
		}
		Assert.assertTrue("got expected unbound param err", gotExpectedErr); 
		dbConn.close();
		dbConn = Util.makeDbConn(); // reconnect after error

		// remake statement, do both binds
		pst = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=? and str_val=?");
		pst.setInt(1, iINT_VAL1);
		pst.setString(2, "abcd");
		pst.setFetchSize(0);
		ResultSet rs = pst.executeQuery();
		rs.next();
		Assert.assertTrue("rs", rs.getInt(1) == iINT_VAL1);
		Assert.assertTrue("rs", rs.getString(2).equals("abcd"));
		Assert.assertTrue("rs", rs.getFloat(3) == 47.42F);
		int rows = 1;
		while (rs.next()) 
			rows++;
		Assert.assertTrue("rows #", rows == ROWS);
		
		// this q ret no rows
		pst.setInt(1, iINT_VAL3);
		rs = pst.executeQuery();
		Assert.assertTrue("", !rs.next());
		
		// this query will return one row
		pst.setInt(1, iINT_VAL2);
		rs = pst.executeQuery();
		Assert.assertTrue("", rs.next());
		Assert.assertTrue("", !rs.next());

		pst.clearParameters();
		rs.close();
		rs.close();
		
		pst.setInt(1, iINT_VAL1);
		pst.setString(2, "abcd");
		rs = pst.executeQuery();
		rows = 0;
		while (rs.next()) 
			rows++;
		Assert.assertTrue("rows #", rows == ROWS);
		cleanTable(dbConn.createStatement(), sID_START, 20, true);
	}	

	@Test
	public void test_callable_st() throws IOException, SQLException{
		if (isMySQL) //Uncomment this once the OUT BIND param supported is added.
			return;

		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + sID_START + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
		dbConn.commit();
		CallableStatement cst;
		if (isMySQL)
		     cst = dbConn.prepareCall("CALL sp_test1(?,?,?,?);");
		else
		     cst = dbConn.prepareCall("BEGIN sp_test1(?,?,?,?); END;");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.executeUpdate();
		Assert.assertTrue("second outbing param", cst.getInt(2) == iINT_VAL1);
		Assert.assertTrue("third outbing param", cst.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst.getFloat(4) == 47.42F);
		
		cst.clearParameters();
		st.executeUpdate("delete from " + table + " where int_val=" + sINT_VAL2);
		st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + 1) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)");
		
		cst.setInt(1, iID_START + 1);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.executeUpdate();
		Assert.assertTrue("second outbing param", cst.getInt(2) == iINT_VAL2);
		Assert.assertTrue("third outbing param", cst.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst.getFloat(4) == 47.42F);
		
		cleanTable(st, sID_START, 20, true);
	}	

	@Test
	public void test_simultan_st_1() throws IOException, SQLException{
		Statement st1 = dbConn.createStatement();
		Statement st2 = dbConn.createStatement();
		cleanTable(st1, sID_START, 20, false);
		final int ROWS = 10;
		for (int i = 0; i < ROWS; i++)
			Assert.assertTrue("Insert row", st1.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'row " + (i + 1) + "', 0, 47.42, null, null, null)") == 1);
		Assert.assertTrue("Insert row", st1.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + "," + sINT_VAL2 + ",'row " + (ROWS + 1) + "', 0, 47.42, null, null, null)") == 1);
		dbConn.commit();
		
		st1.setFetchSize(ROWS / 3);
		ResultSet rs1 = st1.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1);
		ResultSet rs2 = st2.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1 + " order by str_val");
		rs2.next();
		Assert.assertTrue("rs2 first row value is as expected", rs2.getString(2).equals("row 1"));
		Statement st3 = dbConn.createStatement();
		ResultSet rs3 = st3.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1);
		while (rs3.next()) {};
		Statement st4 = dbConn.createStatement();
		ResultSet rs4 = st4.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1);
		Statement st5 = dbConn.createStatement();
		ResultSet rs5 = st5.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1);
		
		Assert.assertTrue("rs2", rs2.getInt(1) == iINT_VAL1);
		rs1.next();
		Assert.assertTrue("rs1", rs1.getInt(1) == iINT_VAL1);
		rs4.next();
		Assert.assertTrue("rs4", rs4.getInt(1) == iINT_VAL1);
		rs5.next();
		Assert.assertTrue("rs5", rs5.getInt(1) == iINT_VAL1);
		
		Assert.assertTrue("rs2 can go next() " + (ROWS / 3) + " times", moveRS(rs2, (ROWS / 3)) == (ROWS / 3));
		try {
			Assert.assertTrue("rs2 after few next()s", rs2.getInt(1) == iINT_VAL1);
		} catch (IndexOutOfBoundsException e) {
			Assert.fail("rs2 after few next()s - rs2 should return more then " + (ROWS / 3) + "rows");
		}
		
		Assert.assertTrue("rs2 count", (1 + (ROWS / 3) + moveRS(rs2, ROWS)) == ROWS);
		Assert.assertTrue("rs2 at end", !rs2.next());

		Assert.assertTrue("rs4 opened", rs4.next());
		ResultSet rs4_1 = st4.executeQuery("select int_val, str_val from " + table + " where int_val=" + sINT_VAL1);
		try {
			rs4.next();
			Assert.fail("rs4 closed after statement re-execute");
		} catch(SQLException e) {
			Assert.assertTrue("rs4 closed after statement re-execute", true);
		}

		Assert.assertTrue("rs4_1 opened", rs4_1.next());
		st4.executeUpdate("delete from " + table + " where int_val=" + sINT_VAL1);
		try {
			rs4_1.next();
			Assert.fail("rs4_1 closed after statement re-execute");
		} catch(SQLException e) {
			Assert.assertTrue("rs4_1 closed after statement re-execute", true);
		}
		
		st3.executeUpdate("delete from " + table + " where int_val=" + sINT_VAL2);
		dbConn.commit();
		cleanTable(st3, sID_START, 20, true);
	}
	
	@Test
	public void test_simultan_st_2() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st1 = dbConn.createStatement();
		cleanTable(st1, sID_START, 20, false);
		final int ROWS = 10;
		for (int i = 0; i < ROWS; i++)
			Assert.assertTrue("Insert row", st1.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'row " + (i + 1) + "', 0, 47.42, null, null, null)") == 1);
		Assert.assertTrue("Insert row", st1.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		dbConn.commit();
		
		Statement st2 = dbConn.createStatement();
		st1.setFetchSize(2);
		ResultSet rs1 = st1.executeQuery("select int_val, str_val, float_val from " + table + " where int_val=" + sINT_VAL1);
		moveRS(rs1, 4);
		ResultSet rs2 = st2.executeQuery("select int_val, str_val, float_val from " + table + " where int_val=" + sINT_VAL1);
		moveRS(rs2, 6);
		Statement st3 = dbConn.createStatement();
		ResultSet rs3 = st3.executeQuery("select int_val, str_val, float_val from " + table + " where int_val=" + sINT_VAL1);
		rs3.next();
		Assert.assertTrue("can move rs2_2 4 rows", moveRS(rs2, 5) == 4);
		Statement st4 = dbConn.createStatement();
		ResultSet rs4 = st4.executeQuery("select int_val, str_val, float_val from " + table + " where int_val=" + sINT_VAL1);
		rs4.next();
		Statement st5 = dbConn.createStatement();
		ResultSet rs5 = st5.executeQuery("select int_val, str_val, float_val from " + table + " where int_val=" + sINT_VAL1);
		rs5.next();
		
		PreparedStatement pst1 = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=?");
		pst1.setInt(1, iINT_VAL1);
		ResultSet rsp1 = pst1.executeQuery();
		rsp1.next();
		PreparedStatement pst2 = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=?");
		pst2.setInt(1, iINT_VAL1);
		ResultSet rsp2 = pst2.executeQuery();
		rsp2.next();
		PreparedStatement pst3 = dbConn.prepareStatement("select int_val, str_val, float_val from " + table + " where int_val=?");
		pst3.setInt(1, iINT_VAL1);
		ResultSet rsp3 = pst3.executeQuery();
		rsp3.next();
		
		CallableStatement cst = dbConn.prepareCall("BEGIN sp_test1(?,?,?,?); END;");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.executeUpdate();
		
		Assert.assertTrue("rs3 still open", rs3.next());
		Assert.assertTrue("rsp2 still open", rsp2.next());
		Assert.assertTrue("rsp1 still open", rsp1.next());
		Assert.assertTrue("rsp1 still open", rsp1.next());
		
		rs3.close();
		
		Assert.assertTrue("rsp1 still open", rsp1.next());
		pst1.close();
		try {
			rsp1.next();
			Assert.fail("rsp1 closed");
		} catch (SQLException e) {
			Assert.assertTrue("rsp1 closed", true);
		}

		Assert.assertTrue("rs1 still open before closing statement", rs1.next());

		// This tests that internally JDBC doesn't keep hard references to the result set.
		// System.gc() is just a hint, it doesn't force the GC, so this cannot be tested reliably. However
		// in debugger, when running step-by-step, System.gc() seems to always invoke the GC
		/*
		System.out.println("Opened record sets: " + ((HeraConnection)dbConn).statementsCount());
		st1 = null;
		System.gc();
		// there is still rs1 around, so gc won't clean it up
		System.out.println("Opened record sets (UNchanged): " + ((HeraConnection)dbConn).statementsCount());
		rs1 = null;
		System.gc();
		// now gc could clean it up
		System.out.println("Opened record sets (should be -1): " + ((HeraConnection)dbConn).statementsCount());
		*/
		
		Assert.assertTrue("rs4 opened", rs4.next());
		st4.close();
		try {
			rs4.next();
			Assert.fail("rs4 closed");
		} catch (SQLException e) {
			Assert.assertTrue("rs4 closed", true);
		}
		
		Assert.assertTrue("rs5 opened", rs5.next());
		st5.executeUpdate("delete from " + table + " where int_val=" + sINT_VAL1);
		try {
			rs5.next();
			Assert.fail("rs5 closed");
		} catch (SQLException e) {
			Assert.assertTrue("rs5 closed", true);
		}
		cleanTable(st5, sID_START, 20, false);
	}
	
	@Test
	public void test_dates_as_strings() throws IOException, SQLException{
		long now = System.currentTimeMillis();
		int dateStrId = iID_START; //555666;
		if (isMySQL) {
			now = now/1000*1000; // slice off milliseconds
		}
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst2;
		if (!isMySQL) {
			pst2 = dbConn.prepareStatement("insert into " + table + " (id, date_val, time_val, timestamp_val) " + 
		"values (?,to_date(?, 'yyyy-mm-dd'), to_date(?, 'hh24:mi:ss'), to_timestamp(?, 'yyyy-mm-dd hh24:mi:ss.FF3'))");
		} else {
			pst2 = dbConn.prepareStatement("insert into " + table + " (id, date_val, time_val, timestamp_val) " + 
		"values (?, str_to_date(?,'%Y-%m-%d'), cast(? as time), str_to_date(?,'%Y-%m-%d %H:%i:%s.%f'))");
		}
		
		pst2.setInt(1, dateStrId);
		Date date = new Date(now - (now % 1000));
		Time time = new Time(now - (now % 1000));
		Timestamp tmst = new Timestamp(now);
		pst2.setString(2, date.toString());
		pst2.setString(3, time.toString());
		pst2.setString(4, tmst.toString());
		try {
			pst2.executeUpdate();
			dbConn.commit();
			Assert.assertTrue("Update fine", true);
		} catch(SQLException e) {
			Assert.fail("Update fine");
		}
		
		PreparedStatement pst1;
		if (!isMySQL) {
			pst1 = dbConn.prepareStatement("select id, to_char(date_val, 'YYYY-MM-DD'), to_char(time_val, 'HH24:MI:SS'), " +
				"to_char(timestamp_val, 'yyyy-mm-dd hh24:mi:ss.FF3') from " + table + " where id=?");
		} else {
			pst1 = dbConn.prepareStatement("select id, DATE_FORMAT(date_val, '%Y-%m-%d'), DATE_FORMAT(time_val, '%H:%i:%s'), " +
				"DATE_FORMAT(timestamp_val, '%Y-%m-%d %H:%i:%s.%f') from " + table + " where id=?");
		}
		pst1.setInt(1, dateStrId);
		ResultSet rs = pst1.executeQuery();
		rs.next();
		Assert.assertTrue("Check date", rs.getString(2).equals(date.toString()));
		Assert.assertTrue("Check time", rs.getString(3).equals(time.toString()));
		//System.out.println("orig ts "+tmst.toString());
		//System.out.println("db ts "+rs.getString(4));
		//System.out.flush();
		//mysql orig ts 2019-06-21 16:15:02.0
		//mysql   db ts 2019-06-21 16:15:02.000000
		Assert.assertTrue("Check timestamp", rs.getString(4).equals(tmst.toString()) || 
				rs.getString(4).equals(tmst.toString() + "00000")||
				rs.getString(4).equals(tmst.toString() + "00") /*this is for if millisecs is 0*/);
		
		cleanTable(st, sID_START, 20, true);
	}

	@Test
	public void test_dates() throws IOException, SQLException{
		long now = System.currentTimeMillis();
		if (isMySQL) 
			now = now - (now % 1000);
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		PreparedStatement pst2 = dbConn.prepareStatement("insert into " + table + " (id, date_val, time_val, timestamp_val, timestamp_tz_val) values (?, ?, ?, ?, ?)");
		pst2.setInt(1, iID_START);
		Date date = new Date(now);
		Time time = new Time(now);
		Timestamp tmst = new Timestamp(now);
		Timestamp tmst_tz = new Timestamp(now);
		Calendar tz = new GregorianCalendar(TimeZone.getTimeZone("GMT-6:00"));
		pst2.setDate(2, date);
		pst2.setTime(3, time);
		pst2.setTimestamp(4, tmst);
		pst2.setTimestamp(5, tmst_tz, tz);
		try {
			pst2.executeUpdate();
			dbConn.commit();
			Assert.assertTrue("Update fine", true);
		} catch(SQLException e) {
			Assert.fail("Update fail");
		}
		
		PreparedStatement pst1 = dbConn.prepareStatement("select id, date_val , time_val, timestamp_val, timestamp_tz_val from " + table + " where id=?");
		pst1.setInt(1, iID_START);
		ResultSet rs = pst1.executeQuery();
		rs.next();
		
		date.setTime(date.getTime() / 1000 * 1000); // trim the millis
		time.setTime(time.getTime() / 1000 * 1000); // trim the millis
		
		// trim the millis, because gooracle driver ignore the millis
		tmst.setTime(tmst.getTime() / 1000 * 1000); 
		tmst_tz.setTime(tmst_tz.getTime() / 1000 * 1000); 
		
		Assert.assertTrue("Check date", rs.getDate(2).toString().equals(date.toString()));
		Assert.assertTrue("Check time", rs.getTime(3).equals(time));
		Assert.assertTrue("Check timestamp", rs.getTimestamp(4).equals(tmst));

		if (false) {
			// with gooracle timezone is ignored, always uses local time 
			Assert.assertTrue("Check timestamp TZ", rs.getTimestamp(5, tz).equals(tmst_tz));
			Assert.assertTrue("Check timestamp TZ offset", rs.getTimestamp(5, tz).getTimezoneOffset() == tmst_tz.getTimezoneOffset());
	
			tz = new GregorianCalendar(TimeZone.getTimeZone("GMT-6:00"));
			Assert.assertTrue("Check timestamp TZ default", !rs.getTimestamp(4, tz).equals(tmst));
			TimeZone tz_la = TimeZone.getTimeZone("America/Los_Angeles");
			if (tz_la.inDaylightTime(new Date(now))) {
				tz = new GregorianCalendar(TimeZone.getTimeZone("GMT-7:00"));
			} else {
				tz = new GregorianCalendar(TimeZone.getTimeZone("GMT-8:00"));
			}
			Assert.assertTrue("Check timestamp TZ default gtm -8", rs.getTimestamp(4, tz).equals(tmst));
			Assert.assertTrue("Check timestamp TZ default gtm -8 current", rs.getTimestamp(4, null).equals(tmst));
		}
		
		cleanTable(st, sID_START, 20, true);
	}	
	
	// Worker needs to implement sending keep-alive (HERA_STILL_EXECUTING)
	@Ignore @Test
	public void test_timeout() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id) values (" + iID_START + ")") == 1);
		dbConn.commit();

		PreparedStatement pst = dbConn.prepareStatement("select id from " + table + " where id=? for update");
		pst.setInt(1, iID_START);
		pst.executeQuery();

		Properties props = new Properties();
		Connection dbConn2 = DriverManager.getConnection("jdbc:hera:" + host, props);
		PreparedStatement pst2 = dbConn2.prepareStatement("select id from " + table + " where id=? for update");
		pst2.setInt(1, iID_START);
		long start = System.currentTimeMillis();
		try {
			pst2.executeQuery();
			Assert.fail("query select for update expected to fail with timeout");
		} catch (HeraTimeoutException ex) {
			Assert.assertTrue("Query timeout after " + (System.currentTimeMillis() - start) + "ms", true);			
		}

		Assert.assertTrue("Connection 2 closed", dbConn2.isClosed());
		dbConn.rollback();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	

	@Test
	public void test_boolean() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBoolean(2, true);
		pst_insert.executeUpdate();
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setBoolean(2, false);
		pst_insert.executeUpdate();
		
		PreparedStatement pst = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst.setInt(1, iID_START);
		ResultSet rs = pst.executeQuery();
		rs.next();
		Assert.assertTrue("Bolean is true", rs.getBoolean(2) == true);
		pst.setInt(1, iID_START + 1);
		rs = pst.executeQuery();
		rs.next();
		Assert.assertTrue("Bolean is false", rs.getBoolean(2) == false);
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	
	
	@Test
	public void test_uniqueConstraint(){
		try{
		
			Statement st = dbConn.createStatement();
			cleanTable(st, sID_START, 20, false);
			dbConn.commit();
		
			PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (?, ?)");
			pst_insert.setInt(1, iID_START);
			pst_insert.setBoolean(2,  true);
			pst_insert.executeUpdate();
			pst_insert.setInt(1, iID_START);
			pst_insert.setBoolean(2, false);
			pst_insert.executeUpdate();
			Assert.fail("Insert statement should throw SQLException with constraint violation");
			
			cleanTable(st, sID_START, 20, false);
			dbConn.commit();
		} catch(SQLException e) {
			// SQLState not implemented for MySQL
			if (isMySQL) 
				return;
			Assert.assertEquals("23000", e.getSQLState());
		}
		
	}
	
	@Test
	public void test_invalidTable(){
		try{
		
			Statement st = dbConn.createStatement();
			cleanTable(st, sID_START, 20, false);
			dbConn.commit();
		
			PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + "1 (id, in_val) values (?, ?)");
			pst_insert.setInt(1, iID_START);
			pst_insert.setBoolean(2,  true);
			pst_insert.executeUpdate();
			Assert.fail("Insert statement into invalid table should throw SQLException");
			
			cleanTable(st, sID_START, 20, false);
			dbConn.commit();
		} catch(SQLException e) {
			// SQLState not implemented for MySQL
			if (isMySQL) 
				return;
			Assert.assertEquals("42000", e.getSQLState());
		}
		
	}
	

	@Test	
	public void test_metadata() throws IOException, SQLException{
//		if (isMySQL) 
//			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " " +
					"(id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values " +
					"(" + iID_START + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)") == 1);
		
		ResultSet rs = st.executeQuery("select id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val " +
				"from " + table + " where id=" + iID_START);
		ResultSetMetaData meta = rs.getMetaData();
		Assert.assertTrue("Column name id", meta.getColumnName(2).equalsIgnoreCase("int_val"));
		Assert.assertTrue("Column name str_val", meta.getColumnName(3).equalsIgnoreCase("str_val"));

		Assert.assertTrue("One row found", rs.next());
		Assert.assertTrue("id value as expected", rs.getInt("id") ==  iID_START);
		Assert.assertTrue("str_val value as expected", rs.getString("str_val").equals("abcd"));
		if (isMySQL) {
			Assert.assertTrue("First col type as expected", meta.getColumnType(1) == Types.INTEGER);
			Assert.assertTrue("Third col type as expected", meta.getColumnType(3) == Types.VARCHAR);
			Assert.assertTrue("Fifth col type as expected", meta.getColumnType(5) == Types.FLOAT);
			Assert.assertTrue("6th col type as expected", meta.getColumnType(6) == Types.BLOB);
			Assert.assertTrue("7th col type as expected", meta.getColumnType(7) == Types.BLOB);
			Assert.assertTrue("8th col type as expected", meta.getColumnType(8) == Types.CLOB);
		} else {
			Assert.assertTrue("First col type as expected", meta.getColumnType(1) == Types.NUMERIC);
			Assert.assertTrue("Third col type as expected", meta.getColumnType(3) == Types.VARCHAR);
			Assert.assertTrue("Fifth col type as expected", meta.getColumnType(5) == Types.NUMERIC);
			Assert.assertTrue("6th col type as expected", meta.getColumnType(6) == Types.VARBINARY);
			Assert.assertTrue("7th col type as expected", meta.getColumnType(7) == Types.BLOB);
			Assert.assertTrue("8th col type as expected", meta.getColumnType(8) == Types.CLOB);
		}

		System.out.println("" + meta.getPrecision(5) + "-" + meta.getScale(5)) ;
		// Assert.assertTrue("Third col size as expected", meta.getColumnDisplaySize(3) == 256);
		if (!isMySQL) { // MySQL returns max_int precission
			Assert.assertTrue("5th col precision as expected", meta.getPrecision(5) == 38);
		}
		Assert.assertTrue("5th col scale as expected", meta.getScale(5) == 10);
		
		cleanTable(st, sID_START, 20, true);
	}

	@Test
	public void test_2psts() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBoolean(2, true);
		pst_insert.executeUpdate();
		
		PreparedStatement pst1 = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		PreparedStatement pst2 = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst1.setInt(1, iID_START);
		pst2.setInt(1, iID_START);
		ResultSet rs1 = pst1.executeQuery();
		ResultSet rs2 = pst2.executeQuery();
		rs1.next();
		Assert.assertTrue("first elem", rs1.getInt("id") == iID_START);
		rs2.close();
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	

	@Test
	public void test_extra_bind() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setInt(2, 1);
		pst_insert.executeUpdate();
		
		PreparedStatement pst1 = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst1.setInt(1, iID_START);
		pst1.setInt(2, iID_START);
		try {
			pst1.executeQuery();
			Assert.fail("Statement should have failed for wrong number of binds");
		} catch (SQLException e) {
			dbConn.rollback();
		}
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	

	@Test
	public void test_nulls() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val, str_val, raw_val) values (? , ?, ?, ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBigDecimal(2, null);
		pst_insert.setCharacterStream(3, null, 1);
		pst_insert.setBinaryStream(4, null, 1);
		pst_insert.executeUpdate();
		Assert.assertTrue("Update with null values worked", true);
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}	
	
	@Test	
	public void test_rowid() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		CallableStatement cst = dbConn.prepareCall("insert into " + table + " (id) values (?) returning rowid into ?");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.executeUpdate();
		String str = cst.getString(2);
		Assert.assertTrue("got rowid", !str.isEmpty());
		Assert.assertTrue("rowid not null", !cst.wasNull());
		cleanTable(st, sID_START, 20, true);
	}	

	@Test	
	public void test_null_outparam() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		CallableStatement cst = dbConn.prepareCall("insert into " + table + " (id) values (?) returning str_val into ?");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.executeUpdate();
		String str = cst.getString(2);
		Assert.assertTrue("out para was null", cst.wasNull());
		cleanTable(st, sID_START, 20, true);
	}	

	@Test	
	public void test_binary_strings() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Delete all", st.executeUpdate("delete from " + table + " where id=" + sID_START) == 0);
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		
		char[] chars = {(char) 0, (char) 0};
		String str = new String(chars);
		pst_insert.setString(2, str);
		pst_insert.executeUpdate();
		
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setString(2, "row 2\0part 2");
		pst_insert.executeUpdate();
				
		dbConn.commit();
		
		ResultSet rs = st.executeQuery("select str_val from " + table + " order by id");
		Assert.assertTrue("Has at least one row", rs.next());
		String str2 = rs.getString(1);
		Assert.assertTrue("Binary 0 (zero) inserted", str.equals(str2));
		Assert.assertTrue("Has a second row", rs.next());
		str2 = rs.getString(1);
		Assert.assertTrue("Full string inserted", str2.equals("row 2\0part 2"));
				
		cleanTable(st, sID_START, 20, true);
	}
	
	@Test	
	public void test_string_whitespace() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Delete all", st.executeUpdate("delete from " + table + " where id=" + sID_START) == 0);
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, str_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		
		String str = new String(" abc\0  ");
		pst_insert.setString(2, str);
		pst_insert.executeUpdate();
		dbConn.commit();
		
		ResultSet rs = st.executeQuery("select str_val from " + table + " order by id");
		Assert.assertTrue("Has at least one row", rs.next());
		String str2 = rs.getString(1);
		Assert.assertTrue("String inserted fine", str2.equals(str));
		cleanTable(st, sID_START, 20, true);
	}

	@Test	
	public void test_string_outparam() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		CallableStatement cst = dbConn.prepareCall("insert into " + table + " (id, int_val, str_val) values (?, ?, ?) returning str_val into ?");
		cst.setInt(1, iID_START);
		cst.setInt(2, 777888);
		String str = new String(" abc\0  ");
		cst.setString(3, str);
		cst.registerOutParameter(4, 0);
		cst.executeUpdate();
		String str2 = cst.getString(4);
		Assert.assertTrue("third outbing param", str2.equals(str));
		cleanTable(st, sID_START, 20, true);
	}	

	@Test	
	public void test_number_format() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Delete all", st.executeUpdate("delete from " + table + " where id=" + sID_START) == 0);
		
		double doubleval =  iINT_VAL1 + .55; 
		Assert.assertTrue("Insert row", st.executeUpdate("insert into " + table + " (id, float_val) values (" + (iID_START) + "," + doubleval + ")") == 1);
		dbConn.commit();

		ResultSet rs = st.executeQuery("select float_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertEquals("rs=" + rs.getDouble(1),
				(int) rs.getDouble(1), iINT_VAL1.intValue());
		cleanTable(st, sID_START, 20, true);
	}

	@Test
	public void test_getBinaryStream() throws IOException, SQLException {
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);

		byte[] testByte = new byte[256];

		for (int i = 0; i < 256; i++) {
			testByte[i] = (byte) i;
		}

		InputStream is = new ByteArrayInputStream(testByte);
		PreparedStatement pst_insert = dbConn
				.prepareStatement("insert into " + table + " (id, raw_val) values (? , ?)");

		pst_insert.setInt(1, iID_START);
		pst_insert.setBinaryStream(2, is, testByte.length);
		pst_insert.executeUpdate();

		ResultSet rs = st
				.executeQuery("select raw_val from " + table + " where id="
						+ iID_START);
		rs.next();

		// Null Test
		Assert.assertTrue("getBinaryStream",
				rs.getBinaryStream("raw_val") != null);

		InputStream is_2 = rs.getBinaryStream("raw_val");
		byte[] tempByte = new byte[256];
		is_2.read(tempByte, 0, 256);

		Assert.assertTrue("Comparing 2 byte arrays",
				Arrays.equals(tempByte, testByte));
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}

	//Commenting all test-cases related to execute() API, since it doesn't work when R/W split is enabled.  
	@Test
	public void test_execute_insert() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		int rows = 0;
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBoolean(2, true);
		boolean execute = pst_insert.execute();
		rows++;
		Assert.assertTrue("Execute is false", execute == false);
		Assert.assertTrue("Get update count ", pst_insert.getUpdateCount() == rows);
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setBoolean(2, false);

		boolean execute2 = pst_insert.execute();
		rows++;

		Assert.assertTrue("Execute is false", execute2 == false);
		pst_insert.getUpdateCount();

		//testing delete
		PreparedStatement pst_delete = dbConn.prepareStatement("delete from " + table + " where id >= " + sID_START + " and id < " + (Integer.parseInt(sID_START) + 20));
		execute_delete(pst_delete,rows,true);
	}

	@Test
	public void test_execute_select() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBoolean(2, true);
		boolean execute = pst_insert.execute(); 
		Assert.assertTrue("Execute is false", execute == false);
		Assert.assertTrue("Get update count ", pst_insert.getUpdateCount() == 1);
		pst_insert.setInt(1, iID_START + 1);
		pst_insert.setBoolean(2, false);
		boolean execute2 = pst_insert.execute();
		Assert.assertTrue("Execute is false", execute2 == false);
		Assert.assertTrue("Get update count ", pst_insert.getUpdateCount() == 1);
			
		
		PreparedStatement pst = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst.setInt(1, iID_START);
		pst.execute();
		Assert.assertTrue("Execute is true", pst.execute() == true);
		ResultSet rs = pst.getResultSet();
		rs.next();
		Assert.assertTrue("Boolean is true", rs.getBoolean(2) == true);
		pst.setInt(1, iID_START + 1);
		rs = pst.executeQuery();
		rs.next();
		Assert.assertTrue("Boolean is false", rs.getBoolean(2) == false);
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}
	
	@Test
	public void test_execute_update() throws IOException, SQLException {
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		Assert.assertTrue("Delete all", st.executeUpdate("delete from " + table + " where id=" + sID_START) == 0);
		
		final int ROWS = 10;
		int count = 0;
		for (int i = 0; i < ROWS; i++) {
			PreparedStatement pst = dbConn.prepareStatement("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + i) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
			boolean exec = pst.execute();
			Assert.assertTrue("Execute is false", exec == false);
			pst.getUpdateCount();
			count++;
		}	
		dbConn.commit();
		
		PreparedStatement pst_update = dbConn.prepareStatement("update " + table + " set str_val='xyz' where int_val=" + sINT_VAL1);
		boolean exec = pst_update.execute();
		Assert.assertTrue("Execute is false", exec == false);
		Assert.assertTrue("Update " + ROWS + " rows",pst_update.getUpdateCount() == ROWS);
		
		PreparedStatement pst = dbConn.prepareStatement("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + ROWS) + ", " + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
		exec = pst.execute();
		Assert.assertTrue("Execute is false", exec == false);
		Assert.assertTrue("Insert row",pst.getUpdateCount() == 1);		
		
		PreparedStatement pst_update2 = dbConn.prepareStatement("update " + table + " set str_val='xyz 2' where int_val=" + sINT_VAL1);
		exec = pst_update2.execute();
		Assert.assertTrue("Execute is false", exec == false);
		Assert.assertTrue("Update " + (ROWS + 1) + " rows",pst_update2.getUpdateCount() == ROWS + 1);
		dbConn.rollback();
		
		PreparedStatement pst_update3 = dbConn.prepareStatement("update " + table + " set str_val='xyz 3' where int_val=" + sINT_VAL1);
		exec = pst_update3.execute();
		Assert.assertTrue("Execute is false", exec == false);
		Assert.assertTrue("Update " + ROWS + " rows", pst_update3.getUpdateCount() == ROWS);
		cleanTable(st, sID_START, 20, true);
	} 
	
	@Ignore @Test 
	public void test_execute_selectupdate() throws IOException, SQLException {
		if (isMySQL) 
			return;
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id) values (" + iID_START + ")");
		boolean exec = pst_insert.execute();
		Assert.assertTrue("Execute is ", exec == false);
		Assert.assertTrue("Insert row", pst_insert.getUpdateCount() == 1);
		dbConn.commit();

		PreparedStatement pst = dbConn.prepareStatement("select id from " + table + " where id=? for update");
		pst.setInt(1, iID_START);
		pst.execute();
		Assert.assertTrue("Execute is ", pst.execute() == true);

		Properties props = new Properties();
		Connection dbConn2 = DriverManager.getConnection("jdbc:hera:" + host, props);
		PreparedStatement pst2 = dbConn2.prepareStatement("select id from " + table + " where id=? for update");
		pst2.setInt(1, iID_START);
		long start = System.currentTimeMillis();
		try {
			pst2.execute();
			Assert.fail("query select for update expected to fail with timeout");
		} catch (HeraTimeoutException ex) {
			Assert.assertTrue("Query timeout after " + (System.currentTimeMillis() - start) + "ms", true);			
		}

		Assert.assertTrue("Connection 2 closed", dbConn2.isClosed());
		dbConn.rollback();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
	}

	
	@Test
	public void test_execute_autoCommit() throws IOException, SQLException {
		//connect,set autocommit on,insert,disconnect,select,then data will be there

		dbConn.setAutoCommit(true);
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		PreparedStatement pst = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst.setInt(1, iID_START);
		pst.setBoolean(2, true);
		boolean exec = pst.execute();
		Assert.assertTrue("Execute is true", exec == false);
		Assert.assertTrue("Number of rows inserted", pst.getUpdateCount() == 1);
		dbConn.close();
		
		//reopen the connection and see if the above inserted row exists
		Properties props = new Properties();
		dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);
		st = dbConn.createStatement();
		
		//select for the row, assert that the data is there 
		PreparedStatement pst_select = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst_select.setInt(1, iID_START);
		pst_select.execute();
		ResultSet rs = pst_select.getResultSet();
		if(rs != null) {
			rs.next();
			Assert.assertTrue("Boolean is true", rs.getBoolean(2) == true);
		}
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
	}
	
	@Test
	public void test_execute_nonAutoCommit() throws IOException, SQLException{
		//connect,set autocommit off,insert,disconnect,select,then data will not be there 
		
		dbConn.setAutoCommit(false);
		
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		
		PreparedStatement pst = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst.setInt(1, iID_START);
		pst.setBoolean(2, true);
		boolean exec = pst.execute();
		Assert.assertTrue("Execute is true ", exec == false);
		Assert.assertTrue("Number of rows inserted", pst.getUpdateCount() == 1);
		ResultSet rset = pst.getResultSet();
		if(rset != null) {
			rset.next();
			Assert.assertTrue("Boolean is true", rset.next() == false);
		}
		
		dbConn.close();
		
		//reopen the connection and see if the above inserted row exists
		Properties props = new Properties();
		dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);
		st = dbConn.createStatement();
		//select for the row, assert that the data is there 
		PreparedStatement pst_select = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst_select.setInt(1, iID_START);
		pst_select.execute();
		ResultSet rs = pst_select.getResultSet();
		Assert.assertTrue("Result set null", rs.next() == false);
		
		
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
	}
	
	@Test	
	public void test_execute_bad_sql() throws IOException, SQLException{
		//good SQL DML followed by bad SQL followed by good non-DML followed by bad SQL followed by good SQL DML.

		//good DML SQL
		PreparedStatement pst_insert = dbConn.prepareStatement("insert into " + table + " (id, int_val) values (? , ?)");
		pst_insert.setInt(1, iID_START);
		pst_insert.setBoolean(2, true);
		boolean execute = pst_insert.execute();
		Assert.assertTrue("Execute is false", execute == false);
		
		//Bad SQL
		PreparedStatement pst = dbConn.prepareStatement("not a SQL query");
		try{
			pst.execute();
			Assert.fail("execute: Invalid query should throw");
		} catch (SQLException e) {
			Assert.assertTrue("execute: Invalid query should throw", true);
		}
		
		//Good non DML SQL 
		PreparedStatement pst_select = dbConn.prepareStatement("select id, int_val from " + table + " where id=?");
		pst_select.setInt(1, iID_START);
		pst_select.execute();
		Assert.assertTrue("Execute is true", pst_select.execute() == true);
		ResultSet rs = pst_select.getResultSet();
		rs.next();
		Assert.assertTrue("Boolean is true", rs.getBoolean(2) == true);
		
		//Bad SQL 
		PreparedStatement pst_bad = dbConn.prepareStatement("not a SQL query");
		try{
			pst_bad.execute();
			Assert.fail("execute: Invalid query should throw");
		} catch (SQLException e) {
			Assert.assertTrue("execute: Invalid query should throw", true);
		}
		
		//Good DML SQL 
		PreparedStatement pst_inst = dbConn.prepareStatement("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START+1) + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
		boolean exec = pst_inst.execute();
		Assert.assertTrue("Execute is false", exec == false);
		
	}
	
	//deleting a row using execute
	void execute_delete(PreparedStatement pst_delete, int rows, boolean commit) throws SQLException {
		pst_delete.execute();
		Assert.assertTrue("Number of rows deleted", pst_delete.getUpdateCount() == rows);
		if (commit)
			dbConn.commit();
	}
		
	@Test
	public void test_execute_DMLNoRows() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
			
		PreparedStatement pst_update = dbConn.prepareStatement("update " + table + " set str_val='xyz' where int_val=" + sINT_VAL1);
		boolean exec = pst_update.execute();
		Assert.assertTrue("Execute is false", exec == false);
	}
	
	@Test
	public void test_execute_callable_st() throws IOException, SQLException{			
		if (isMySQL) 
			return;
		LOGGER.debug("+++++++ Begin test test_execute_callable_st()");
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + sID_START + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
		dbConn.commit();
		CallableStatement cst = dbConn.prepareCall("BEGIN sp_test1(?,?,?,?); END;");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.execute();
		Assert.assertTrue("second outbing param", cst.getInt(2) == iINT_VAL1);
		Assert.assertTrue("third outbing param", cst.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst.getFloat(4) == 47.42F);
		
		cst.clearParameters();
		st.execute("delete from " + table + " where int_val=" + sINT_VAL2);
		st.execute("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + 1) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)");
		
		cst.setInt(1, iID_START + 1);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.execute();
		Assert.assertTrue("second outbing param", cst.getInt(2) == iINT_VAL2);
		Assert.assertTrue("third outbing param", cst.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst.getFloat(4) == 47.42F);
				
		cleanTable(st, sID_START, 20, true);
		LOGGER.debug("+++++++ Begin test test_execute_callable_st()");
	}		


	@Test
	public void test_escape() throws IOException, SQLException{
//		CallableStatement st = conn.prepareCall("insert into " + table + " values (?,null, 0, 55, null) returning float_val into ?");
		/*
		CREATE OR REPLACE PROCEDURE sp_test1(
			   p_id IN Number,
			   o_id OUT Number,
			   o_str_val OUT  VARCHAR2,
			   o_float_val OUT number)
		IS
		BEGIN
		 
		  SELECT int_val , str_val, float_val
		  INTO o_id, o_str_val,  o_float_val 
		  FROM  " + table + " WHERE id = p_id;
		 
		END;
		*/
		if (isMySQL) 
			return;

		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + sID_START + "," + sINT_VAL1 + ",'abcd', 0, 47.42, null, null, null)");
		dbConn.commit();
		//CallableStatement cst = dbConn.prepareCall("BEGIN sp_test1(?,?,?,?); END;");
		CallableStatement cst = dbConn.prepareCall("{ call sp_test1(?,?,?,?) }");
		cst.setInt(1, iID_START);
		cst.registerOutParameter(2, 0);
		cst.registerOutParameter(3, 0);
		cst.registerOutParameter(4, 0);
		cst.setEscapeProcessing(true);
		cst.executeUpdate();
		Assert.assertTrue("second outbing param", cst.getInt(2) == iINT_VAL1);
		Assert.assertTrue("third outbing param", cst.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst.getFloat(4) == 47.42F);
		
		st.executeUpdate("delete from " + table + " where int_val=" + sINT_VAL2);
		st.executeUpdate("insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val) values (" + (iID_START + 1) + "," + sINT_VAL2 + ",'abcd', 0, 47.42, null, null, null)");
		
		// the same SQL statement, escape processing will use the cached result
		CallableStatement cst2 = dbConn.prepareCall("{ call sp_test1(?,?,?,?) }");
		cst2.setInt(1, iID_START + 1);
		cst2.registerOutParameter(2, 0);
		cst2.registerOutParameter(3, 0);
		cst2.registerOutParameter(4, 0);
		cst2.setEscapeProcessing(true);
		cst2.executeUpdate();
		Assert.assertTrue("second outbing param", cst2.getInt(2) == iINT_VAL2);
		Assert.assertTrue("third outbing param", cst2.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst2.getFloat(4) == 47.42F);
		
		// the SQL has some extra white-spaces, escape processing will be done again, because it is not in the cache
		CallableStatement cst3 = dbConn.prepareCall("  \t\n\r    { call sp_test1(?,?,?,?) \n \t }\n ");
		cst3.setInt(1, iID_START + 1);
		cst3.registerOutParameter(2, 0);
		cst3.registerOutParameter(3, 0);
		cst3.registerOutParameter(4, 0);
		cst3.setEscapeProcessing(true);
		cst3.executeUpdate();
		Assert.assertTrue("second outbing param", cst3.getInt(2) == iINT_VAL2);
		Assert.assertTrue("third outbing param", cst3.getString(3).equals("abcd"));
		Assert.assertTrue("fourth outbing param", cst3.getFloat(4) == 47.42F);
		
		cleanTable(st, sID_START, 20, true);
	}	
	
	@Test
	public void test_merge() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();

		st.executeUpdate("delete from " + table + "");

		cleanTable(st, sID_START, 20, false);
		st.executeUpdate("merge into " + table + " D using (select " + sID_START + " id," + sINT_VAL1 + " int_val from dual) S "
				+ " ON (D.id = S.id) " + 
				//				"when matched then update set D.int_val=S.int_val " +
				"when not matched then insert (D.id, D.int_val) values (S.id, S.int_val) ");
		//		ResultSet rs = st.executeQuery("select * from " + table + "");
		//		rs.next();

		cleanTable(st, sID_START, 20, true);
	}	

	@Test
	public void test_binary_outbind() throws IOException, SQLException{
		/*
		 * make sure hera.txt has max_out_bind_var_size=1024
		 */
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();

		byte[] testByte = new byte[256];

		for (int i = 0; i < 256; i++) {
			testByte[i] = (byte) i;
		}


		CallableStatement cst = dbConn.prepareCall("begin insert into " + table + " (id, raw_val) values (?,?) returning raw_val into ?; end;");

		cst.setInt(1, iID_START);
		cst.setBytes(2, testByte);
		cst.registerOutParameter(3, java.sql.Types.BINARY); 
		cst.executeUpdate();

		byte[] retBytes = cst.getBytes(3);

		for (int i = 0; i < 256; i++) {
			if (retBytes[i] != testByte[i])
				Assert.fail("Byte index " + i + ": expected '" + testByte[i] + "' got '" + retBytes[i] + "'");
		}
		Assert.assertTrue("Binary outbind OK", true);

		cleanTable(st, sID_START, 20, true);
	} 
	
	@Test
	public void test_OOM() throws IOException, SQLException{
		Properties props = new Properties();
		props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY, "false");
		props.setProperty(HeraClientConfigHolder.SUPPORT_COLUMN_NAMES_PROPERTY, "false");
		HeraClientConfigHolder.clear();
		Connection dbConn2 = DriverManager.getConnection("jdbc:hera:" + host, props);
		Statement st = dbConn2.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn2.commit();
		try {
			ResultSet rs = st.executeQuery("insert into " + table + " (id) values ( " + sID_START + " )");
			boolean b = rs.next();
			Assert.assertTrue("Caught HeraInternalErrorException", false);
		} catch (HeraInternalErrorException ex) {
			Assert.assertTrue("Caught HeraInternalErrorException", true);
		}
		dbConn2.close();
	}
	
	@Test
	public void test_setObject_basic() throws IOException, SQLException{
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val, str_val, clob_val) values (?, ?, ?, ?)");
		pst.setObject(1, iID_START);
		pst.setObject(2, 1234);
		pst.setObject(3, "abc");
		Clob clob = dbConn.createClob();
		clob.setString(1, "xyzw");
		pst.setObject(4, clob);
		try {
			pst.executeUpdate();
		} catch(SQLException e) {
			Assert.fail("Can't insert: " + e.getMessage());
		}
		ResultSet rs = st.executeQuery("select id, int_val, str_val, clob_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertTrue("rs", rs.getInt(1) == iID_START);
		Assert.assertTrue("rs", rs.getInt(2) == 1234);
		Assert.assertTrue("rs", rs.getString(3).equals("abc"));
		Assert.assertTrue("rs", rs.getClob(4).getSubString(1, (int)rs.getClob(4).length()).equals("xyzw"));
		cleanTable(st, sID_START, 20, true);
	}	

	@Test
	public void test_setObject() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val, "
				+ "time_val, timestamp_val, timestamp_tz_val) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		pst.setObject(1, iID_START);
		pst.setObject(2, new BigDecimal(1234));
		pst.setObject(3, "abc");
		pst.setObject(4, new Boolean(false));
		pst.setObject(5, new Float(1.234F));
		pst.setObject(6, (new String("xyz")).getBytes());
		Blob blob = dbConn.createBlob();
		blob.setBytes(1, (new String("Blob value")).getBytes());
		pst.setObject(7, blob);
		Clob clob = dbConn.createClob();
		clob.setString(1, "Clob value");
		pst.setObject(8, clob);
		long now = System.currentTimeMillis() / 1000 * 1000; // truncate the milliseconds
		pst.setObject(9, new Date(now));
		pst.setObject(10, new Time(now));
		pst.setObject(11, new Timestamp(now));
		pst.setObject(12, (Timestamp)null);
		
		try {
			pst.executeUpdate();
		} catch(SQLException e) {
			Assert.fail("Can't insert: " + e.getMessage());
		}
		ResultSet rs = st.executeQuery("select id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val,"
				+ "time_val, timestamp_val, timestamp_tz_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertTrue("int", rs.getInt(1) == iID_START);
		Assert.assertTrue("BigDecimal", rs.getInt(2) == 1234);
		Assert.assertTrue("String", rs.getString(3).equals("abc"));
		Assert.assertTrue("Boolean", rs.getBoolean(4) == false);
		Assert.assertTrue("Float", rs.getFloat(5) == 1.234F);
		Assert.assertTrue("Bytes", rs.getString(6).equals("xyz"));
		
		Assert.assertTrue("Blob", ( new String(rs.getBlob(7).getBytes(1, (int)rs.getBlob(7).length()))).equals("Blob value"));
		Assert.assertTrue("Clob", rs.getString(8).equals("Clob value"));
		Assert.assertTrue("Date", rs.getDate(9).toString().equals((new Date(now)).toString()));
		Assert.assertTrue("Time", rs.getTime(10).toString().equals((new Time(now)).toString()));
		Assert.assertTrue("Timestamp", rs.getTimestamp(11).toString().equals((new Timestamp(now)).toString()));
//		Assert.assertTrue("null", rs.getTimestamp(12) == null);

		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		pst.setObject(1, new Long(iID_START));
		pst.setObject(2, new Short((short) 1234));
		pst.setObject(3, new Character('C'));
		pst.setObject(4, new Byte((byte) 'A'));
		pst.setObject(5, new Double(1.2345));
		try {
			pst.executeUpdate();
		} catch(SQLException e) {
			Assert.fail("Can't insert: " + e.getMessage());
		}
		rs = st.executeQuery("select id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val,"
				+ "time_val, timestamp_val, timestamp_tz_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertTrue("Long", rs.getLong(1) == iID_START);
		Assert.assertTrue("Short", rs.getShort(2) == 1234);
		Assert.assertTrue("Character", rs.getString(3).equals("C"));
		Assert.assertTrue("Byte", rs.getByte(4) == 'A');
		Assert.assertTrue("Double", Math.abs(rs.getDouble(5) - new Double(1.2345)) < 0.0001);

		cleanTable(st, sID_START, 20, true);
		dbConn.commit();
	}	
	
	@Test
	public void test_setObject_Type() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val, "
				+ "time_val, timestamp_val, timestamp_tz_val) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		pst.setObject(1, iID_START, Types.INTEGER);
		pst.setObject(2, "1234", Types.NUMERIC);
		pst.setObject(3, "abc", Types.VARCHAR);
		pst.setObject(4, new Boolean(false), Types.BOOLEAN);
		pst.setObject(5, "1.234F", Types.FLOAT);
		pst.setObject(6, (new String("xyz")).getBytes(), Types.VARBINARY);
		pst.setObject(7, (new String("Blob value")).getBytes(), Types.BLOB);
		pst.setObject(8, "Clob value", Types.CLOB);
		long now = System.currentTimeMillis() / 1000 * 1000; // truncate the milliseconds
		pst.setObject(9, new Date(now).toString(), Types.DATE);
		pst.setObject(10, new Time(now).toString(), Types.TIME);
		pst.setObject(11, new Timestamp(now), Types.TIMESTAMP);
		pst.setObject(12, null, Types.NULL);
		
		try {
			pst.executeUpdate();
		} catch(SQLException e) {
			Assert.fail("Can't insert: " + e.getMessage());
		}
		ResultSet rs = st.executeQuery("select id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val,"
				+ "time_val, timestamp_val, timestamp_tz_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertTrue("int", rs.getInt(1) == iID_START);
		Assert.assertTrue("BigDecimal", rs.getInt(2) == 1234);
		Assert.assertTrue("String", rs.getString(3).equals("abc"));
		Assert.assertTrue("Boolean", rs.getBoolean(4) == false);
		Assert.assertTrue("Float", rs.getFloat(5) == 1.234F);
		Assert.assertTrue("Bytes", rs.getString(6).equals("xyz"));
		
		Assert.assertTrue("Blob", ( new String(rs.getBlob(7).getBytes(1, (int)rs.getBlob(7).length()))).equals("Blob value"));
		Assert.assertTrue("Clob", rs.getString(8).equals("Clob value"));
		Assert.assertTrue("Date", rs.getDate(9).toString().equals((new Date(now)).toString()));
		Assert.assertTrue("Time", rs.getTime(10).toString().equals((new Time(now)).toString()));
		Assert.assertTrue("Timestamp", rs.getTimestamp(11).toString().equals((new Timestamp(now)).toString()));
//		Assert.assertTrue("null", rs.getTimestamp(12) == null);
		
		cleanTable(st, sID_START, 20, true);
		dbConn.commit();
	}	
	
	@Test
	public void test_setObject_timeFormat() throws IOException, SQLException{
		if (isMySQL) 
			return;
		Statement st = dbConn.createStatement();
		cleanTable(st, sID_START, 20, false);
		dbConn.commit();
		
		PreparedStatement pst = dbConn.prepareStatement(
				"insert into " + table + " (id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val, "
				+ "time_val, timestamp_val, timestamp_tz_val) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		pst.setObject(1, iID_START, Types.INTEGER);
		pst.setObject(2, "1234", Types.NUMERIC);
		pst.setObject(3, "abc", Types.VARCHAR);
		pst.setObject(4, new Boolean(false), Types.BOOLEAN);
		pst.setObject(5, "1.234F", Types.FLOAT);
		pst.setObject(6, (new String("xyz")).getBytes(), Types.VARBINARY);
		pst.setObject(7, (new String("Blob value")).getBytes(), Types.BLOB);
		pst.setObject(8, "Clob value", Types.CLOB);
		long now = System.currentTimeMillis() / 1000 * 1000; // truncate the milliseconds
		pst.setObject(9, new Date(now).toString(), Types.DATE);
		String dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(now));
		pst.setObject(10, dt, Types.TIME);
		pst.setObject(11, new Timestamp(now), Types.TIMESTAMP);
		pst.setObject(12, null, Types.NULL);
		
		try {
			pst.executeUpdate();
		} catch(SQLException e) {
			Assert.fail("Can't insert: " + e.getMessage());
		}
		ResultSet rs = st.executeQuery("select id, int_val, str_val, char_val, float_val, raw_val, blob_val, clob_val, date_val,"
				+ "time_val, timestamp_val, timestamp_tz_val from " + table + " where id=" + iID_START);
		rs.next();
		Assert.assertTrue("int", rs.getInt(1) == iID_START);
		Assert.assertTrue("BigDecimal", rs.getInt(2) == 1234);
		Assert.assertTrue("String", rs.getString(3).equals("abc"));
		Assert.assertTrue("Boolean", rs.getBoolean(4) == false);
		Assert.assertTrue("Float", rs.getFloat(5) == 1.234F);
		Assert.assertTrue("Bytes", rs.getString(6).equals("xyz"));
		
		Assert.assertTrue("Blob", ( new String(rs.getBlob(7).getBytes(1, (int)rs.getBlob(7).length()))).equals("Blob value"));
		Assert.assertTrue("Clob", rs.getString(8).equals("Clob value"));
		Assert.assertTrue("Date", rs.getDate(9).toString().equals((new Date(now)).toString()));
		Assert.assertTrue("Time", rs.getTime(10).toString().equals((new Time(now)).toString()));
		Assert.assertTrue("Timestamp", rs.getTimestamp(11).toString().equals((new Timestamp(now)).toString()));
//		Assert.assertTrue("null", rs.getTimestamp(12) == null);
		
		cleanTable(st, sID_START, 20, true);
		dbConn.commit();
	}
	
	@Test
	public void test_execute_autoCommit_exception() throws IOException, SQLException {
		try {
			HeraClientConfigHolder config = new HeraClientConfigHolder(new Properties());
			HeraClientImpl heraClient = (HeraClientImpl) HeraClientFactory.createClient(config, "localhost", "11111");
			Method m = heraClient.getClass().getDeclaredMethod("setServerLogicalName", String.class);
			m.setAccessible(true);		  
			m.invoke(heraClient, new String("foo")); 			
		} catch (Exception e) {
			Assert.assertTrue((e instanceof HeraClientException));
		} 
	}
	
	@Test
	public void test_client_ping() throws IOException, SQLException {
		try {
			HeraConnection heraConn = (HeraConnection)dbConn;
			HeraClient heraClient = heraConn.getHeraClient();
			long start = System.currentTimeMillis();
			heraClient.ping(0);
			System.out.println("ping took (ms):" + (System.currentTimeMillis() - start));

		} catch (Exception e) {
			Assert.assertTrue("expect no exception", false);
		}
	}
	
	@Test
	public void test_blob_ex() throws IOException, SQLException {
		Blob b = new HeraBlob("blob".getBytes());
		try {
			b.getBytes(20, 0);
			Assert.fail("Blob.getBytes should throw if pos is not 1");
		} catch (SQLFeatureNotSupportedException e) {
			
		}
		try {
			b.getBytes(1, "blob".getBytes().length - 2);
			Assert.fail("Blob.getBytes should throw if length is not max");
		} catch (SQLFeatureNotSupportedException e) {
			
		}
	}
	
	@Test
	public void test_isValid_connection() throws IOException, SQLException {
		try {
			Properties props = new Properties();
			Connection dbConn2 = DriverManager.getConnection("jdbc:hera:" + host, props);
			Assert.assertTrue("isValid(0) - no timeout", dbConn2.isValid(0));
			Assert.assertTrue("isValid(10) - 10 s timeout", dbConn2.isValid(10));
			dbConn2.close();
			Assert.assertFalse("isValid(0) false after connection is closed", dbConn2.isValid(0));
			
			// recreate the connection
			dbConn2 = DriverManager.getConnection("jdbc:hera:" + host, props);
			Assert.assertTrue("isValid(0) - no timeout, re-created connection", dbConn2.isValid(0));
			// close the socket from under the JDBC connection
			HeraClient heraClient = ((HeraConnection)dbConn2).getHeraClient();
			heraClient.close();
			// this time isValid() will attempt to send the ping, over a stale connection
			Assert.assertFalse("isValid(0) false after connection got stale", dbConn2.isValid(0));
		} catch (Exception e) {
			Assert.assertTrue("expect no exception", false);
		}
	}
	
}
