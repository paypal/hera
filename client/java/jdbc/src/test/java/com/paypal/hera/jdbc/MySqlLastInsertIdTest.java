package com.paypal.hera.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.paypal.hera.conf.HeraConnectionConfig;
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
	public void cleanUp() throws SQLException {
		dbConn.close();
	}

	@Test
	public void testSetShardHintAsShardIDFail() throws Exception{

		Properties props = new Properties();
		props.setProperty(HeraConnectionConfig.CONNECTION_TIMEOUT_MSECS_PROPERTY, "3000");
		HeraConnection hera = (HeraConnection)dbConn;
		try {
			hera.setShardHint("shardid", "100");
			Assert.fail("should have thrown exception");
		}catch (HeraClientException ex){
			Assert.assertTrue(ex.getMessage(), ex.getMessage().contains("2: HERA-201: shard id out of range"));
		}
		Assert.assertTrue(dbConn.isClosed());
	}

	public int rowCount(Connection dbConn, String msg) throws SQLException {
		PreparedStatement ps = dbConn.prepareStatement("select id from test_mysql_last_insert_id");
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
	public void test_mysql_last_insert_id() throws IOException, SQLException{
		PreparedStatement ps;
		ResultSet rs;
		int count;

		if (!isMySQL) 
			return;

		try {
			PreparedStatement pst = dbConn.prepareStatement("create table test_mysql_last_insert_id ( id int, note varchar(55), autoI int not null auto_increment, primary key (autoI) )");
			pst.executeUpdate();
		} catch (Throwable t) {
			// ignore errors in setup
		}


		rowCount(dbConn, "beforeTableClearing");

		PreparedStatement pst0 = dbConn.prepareStatement("delete from test_mysql_last_insert_id ");
		pst0.executeUpdate();

		count = rowCount(dbConn, "afterTableClearing");
		Assert.assertTrue("make sure delete's lastInsertId did not corrupt connection protocol", (count==0) );
		// setup done

		PreparedStatement pst2 = dbConn.prepareStatement("insert into test_mysql_last_insert_id ( id , note ) values ( ?, ? )", Statement.RETURN_GENERATED_KEYS);
		pst2.setInt(1, 11);
		pst2.setString(2, "eleven");
		pst2.executeUpdate();
		rs = pst2.getGeneratedKeys();
		long id = -1;
		try {
			id = rs.getLong(1);
		} catch (Throwable t) {
			Assert.assertTrue("oops no generated key", false);
		}
		count = rowCount(dbConn, "after1ins");
		Assert.assertTrue("select sees first row", (count==1));
		
		pst2 = dbConn.prepareStatement("insert into test_mysql_last_insert_id ( id , note ) values ( ?, ? )", Statement.RETURN_GENERATED_KEYS);
		pst2.setInt(1, 12);
		pst2.setString(2, "twelve");
		pst2.executeUpdate();
		ResultSet rs3 = pst2.getGeneratedKeys();
		long id3 = -1;
		try {
			id3 = rs3.getLong(1);
		} catch (Throwable t) {
			System.out.println("getting last insert id"+t);
			t.printStackTrace();
			Assert.assertTrue("oops no last insert id", false);
		}
		System.out.println("last insert id are "+id+" "+id3);
		Assert.assertTrue("diff rows inserted, expect diff ids", id3 != id);
		count = rowCount(dbConn, "after2ins");
		Assert.assertTrue("select sees second row", (count==2));

		dbConn.commit(); // hrm. somehow needed to make the row stick

		// some testing to see that the connection and protocol are good
		ps = dbConn.prepareStatement("update test_mysql_last_insert_id set note='Eleven' where id=11");
		int rowsUpdated = ps.executeUpdate();
		System.out.println(rowsUpdated+" rows updated");

		// constraint violation
		boolean caught = false;
		try {
			ps = dbConn.prepareStatement("insert into test_mysql_last_insert_id ( autoI ) values ( "+id3+" )");
			ps.executeUpdate();
		} catch (Throwable T) {
			caught = true;
		}
		System.out.println("was exception caught:"+caught);
		Assert.assertTrue("expected constraint exception", caught);
		count = rowCount(dbConn, "after err");
		Assert.assertTrue("select after err", (count==2));

		ps = dbConn.prepareStatement("delete from test_mysql_last_insert_id where id=12");
		ps.executeUpdate();

		// try with autocommit
		dbConn.setAutoCommit(true);
		rowCount(dbConn, "beforeTableClearing2");

		pst0 = dbConn.prepareStatement("delete from test_mysql_last_insert_id ");
		pst0.executeUpdate();

		count = rowCount(dbConn, "afterTableClearing2");
		Assert.assertTrue("make sure 2delete's lastInsertId did not corrupt connection protocol", (count==0) );
		// setup done

		pst2 = dbConn.prepareStatement("insert into test_mysql_last_insert_id ( id , note ) values ( ?, ? )", Statement.RETURN_GENERATED_KEYS);
		pst2.setInt(1, 11);
		pst2.setString(2, "eleven b");
		pst2.executeUpdate();
		rs = pst2.getGeneratedKeys();
		id = -1;
		try {
			id = rs.getLong(1);
		} catch (Throwable t) {
			Assert.assertTrue("ac: oops no generated key", false);
		}
		count = rowCount(dbConn, "2after1ins");
		Assert.assertTrue("select sees first row", (count==1));
	}
}
