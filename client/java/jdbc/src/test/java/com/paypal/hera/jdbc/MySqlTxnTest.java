package com.paypal.hera.jdbc;

import java.net.*;
import java.nio.file.*;
import java.io.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.*;

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

public class MySqlTxnTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	
	private Connection dbConn;
	private String host;
	private boolean isMySQL;

	
	@Before
	public void setUp() throws Exception {
		Util.makeAndStartHeraMux(null);
		dbConn = Util.makeDbConn();

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
	public void cleanUp() throws SQLException, IOException, InterruptedException {
		dbConn.close();
//		Util.stopMySqlContainer();

	}

	public static String TEST_TABLE = "test_mysql_txn";

	public int rowCount(Connection dbConn, String msg) throws SQLException {
		PreparedStatement ps = dbConn.prepareStatement("select id from "+TEST_TABLE);
		ResultSet rs = ps.executeQuery();
		int count = 0;
		while (rs.next()) {
			System.out.println(msg+" select got id "+rs.getInt(1));
			count++;
		}
		System.out.println(msg+" "+count+" rows fetched from select");
		return count;
	}
	
	
	@Test
	public void test_mysql_txn() throws IOException, SQLException{
		PreparedStatement ps;
		ResultSet rs;
		int count;

		if (!isMySQL) 
			return;

		try {
			PreparedStatement pst = dbConn.prepareStatement("create table "+TEST_TABLE+" ( id int, note varchar(55) )");
			pst.executeUpdate();
		} catch (Throwable t) {
			// ignore errors in setup
		}


		rowCount(dbConn, "beforeTableClearing");

		PreparedStatement pst0 = dbConn.prepareStatement("delete from "+TEST_TABLE+" ");
		pst0.executeUpdate();

		count = rowCount(dbConn, "afterTableClearing");
		Assert.assertTrue("make sure delete's lastInsertId did not corrupt connection protocol", (count==0) );
		// setup done


		Connection dbConn2 = Util.makeDbConn();

		dbConn.setAutoCommit(true);
		PreparedStatement pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
		pst2.setInt(1, 11);
		pst2.setString(2, "eleven");
		pst2.executeUpdate();
		count = rowCount(dbConn2, "otherSeesAutoCommit");
		Assert.assertTrue("other connection sees autocommit", count > 0);
		

		// explicit transaction
		dbConn.setAutoCommit(false);
		//PreparedStatement stxn = dbConn.prepareStatement("start transaction");
		//stxn.executeUpdate();
		pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
		pst2.setInt(1, 12);
		pst2.setString(2, "twelve");
		pst2.executeUpdate();
		count = rowCount(dbConn, "after2ins");
		Assert.assertTrue("select sees second row", (count==2));

		count = rowCount(dbConn2, "other conn before commit");
		Assert.assertTrue("other should not see second row", (count!=2));

		dbConn.commit(); 

		count = rowCount(dbConn2, "other conn after commit");
		Assert.assertTrue("other should see second row", (count==2));

		// explicit rollback
		dbConn.setAutoCommit(false);
		//stxn.executeUpdate();
		pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
		pst2.setInt(1, 14);
		pst2.setString(2, "fourteen");
		pst2.executeUpdate();
		count = rowCount(dbConn, "id14");
		Assert.assertTrue("select sees third row", (count==3));

		count = rowCount(dbConn2, "other conn before commit");
		Assert.assertTrue("other should not see third row", (count==2));

		dbConn.rollback(); 

		count = rowCount(dbConn2, "other conn after rollback");
		Assert.assertTrue("other should see rollback 2 rows", (count==2));
		count = rowCount(dbConn, "id14");
		Assert.assertTrue("select sees rolled back to 2 rows", (count==2));

	}
}
