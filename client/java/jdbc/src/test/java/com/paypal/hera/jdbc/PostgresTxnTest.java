package com.paypal.hera.jdbc;

import com.paypal.hera.client.HeraClientImpl;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

/**
 * 
 * see ClientTest.java for tables setup
 *
 */

public class PostgresTxnTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	
	private static Connection dbConn, dbConn2;
	private static boolean isPostgres;

	
	@BeforeClass
	public static void setUp() throws Exception {
		UtilPostgres.makeAndStartHeraMux(null);
		dbConn = UtilPostgres.makeDbConn();
		// determine database server
		HeraConnection hera = (HeraConnection)dbConn;
		hera.setShardHint("shardid", "0");
		Statement st = dbConn.createStatement();
		try {
			st.executeQuery("SELECT HOST_NAME fROM v$instance");
			isPostgres = false;
			LOGGER.debug("Testing with Oracle");
		} catch (SQLException ex) {
			isPostgres = true;
			LOGGER.debug("Testing with Postgres");
		}
		hera.resetShardHints();

		try {
			LOGGER.info("Re-create table: " + TEST_TABLE);
			try {
				st.executeUpdate("drop table " + TEST_TABLE + "");
				dbConn.commit();
			} catch (SQLException ex) {
				dbConn.rollback();
				LOGGER.debug("table doesn't exists: " + ex.getMessage());
			}
			st.executeUpdate("create table "+ TEST_TABLE + " ( id integer primary key, note varchar(55) )");
			dbConn.commit();
		} catch (Exception ex) {
			dbConn.rollback();
			LOGGER.error("Caught ex during setup (create table):" + ex.getMessage());
			throw ex;
			// ignore errors in setup
		}

		rowCount(dbConn, "beforeTableClearing");
		st.executeUpdate("delete from "+TEST_TABLE+" ");
		Assert.assertTrue("make sure delete's lastInsertId did not corrupt connection protocol", rowCount(dbConn, "afterTableClearing")==0 );
		dbConn.commit();
		// setup done
		LOGGER.info("Setup OK");

	}

	@AfterClass
	public static void cleanUpAll() throws SQLException {
		dbConn.close();
		dbConn2.close();
		LOGGER.info("Done");
	}

	public static String TEST_TABLE = "test_postgres_txn";

	public static int rowCount(Connection dbConn, String msg) throws SQLException {
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
	public void test_postgres_txn() throws SQLException {

		int count;

		if (!isPostgres)
			return;
		//AutoCommit
		dbConn2 = UtilPostgres.makeDbConn();
		dbConn.setAutoCommit(true);
		PreparedStatement pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
		pst2.setInt(1, 11);
		pst2.setString(2, "eleven");
		pst2.executeUpdate();
		count = rowCount(dbConn2, "otherSeesAutoCommit");
		Assert.assertTrue("other connection sees autocommit", count > 0);

		try {
			// explicit transaction
			dbConn.setAutoCommit(false);
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

			// First sql statement failure
			try {
				dbConn.setAutoCommit(false);
				// Primary key constraint violation
				pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
				pst2.setInt(1, 12);
				pst2.setString(2, "twelve");
				pst2.executeUpdate();
				dbConn.commit();
			} catch (SQLException e) {
				dbConn.rollback();
				e.printStackTrace();
			}
			count = rowCount(dbConn, "After rollback");
			Assert.assertTrue("select sees rolled back to 2 rows", (count==2));
			count = rowCount(dbConn2, "other conn after rollback");
			Assert.assertTrue("other should see rollback 2 rows", (count==2));

			// New Txn following failure should succeed
			dbConn.setAutoCommit(false);
			pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
			pst2.setInt(1, 14);
			pst2.setString(2, "fourteen");
			pst2.executeUpdate();
			count = rowCount(dbConn, "after3ins");
			Assert.assertTrue("select sees second row", (count==3));

			count = rowCount(dbConn2, "other conn before commit");
			Assert.assertTrue("other should not see third row", (count!=3));

			dbConn.commit();
			count = rowCount(dbConn2, "other conn after commit");
			Assert.assertTrue("other should see third row", (count==3));

			//Failure with AutoCommit
			dbConn.setAutoCommit(true);
			try {
				// Syntax error
				pst2 = dbConn.prepareStatement("inset into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
				pst2.setInt(1, 15);
				pst2.setString(2, "fifteen");
				pst2.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			count = rowCount(dbConn, "After Failure");
			Assert.assertTrue("select sees rolled back to 3 rows", (count==3));
			count = rowCount(dbConn2, "other conn after failure");
			Assert.assertTrue("other should see rollback 3 rows", (count==3));


			//Failure with AutoCommit
			try {
				// Primary key constraint
				pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
				pst2.setInt(1, 11);
				pst2.setString(2, "eleven");
				pst2.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			count = rowCount(dbConn, "After Failure");
			Assert.assertTrue("select sees rolled back to 3 rows", (count==3));
			count = rowCount(dbConn2, "other conn after failure");
			Assert.assertTrue("other should see rollback 3 rows", (count==3));

			// New Txn following failure should succeed
			dbConn.setAutoCommit(false);
			pst2 = dbConn.prepareStatement("insert into "+TEST_TABLE+" ( id , note ) values ( ?, ? )" );
			pst2.setInt(1, 15);
			pst2.setString(2, "fifteen");
			pst2.executeUpdate();
			count = rowCount(dbConn, "after4ins");
			Assert.assertTrue("select sees fourth row", (count==4));

			count = rowCount(dbConn2, "other conn before commit");
			Assert.assertTrue("other should not see fourth row", (count!=4));

			dbConn.commit();
			count = rowCount(dbConn2, "other conn after commit");
			Assert.assertTrue("other should see fourth row", (count==4));

            // Update
            pst2 = dbConn.prepareStatement("update "+TEST_TABLE+" set id = ? , note = ? where id = ?" );
            pst2.setInt(1, 10);
            pst2.setString(2, "ten");
            pst2.setInt(3, 11);
            pst2.executeUpdate();
            count = rowCount(dbConn, "afterupdate");
            Assert.assertTrue("select sees fourth row", (count==4));
            dbConn.commit();

            count = rowCount(dbConn2, "other conn after commit");
            Assert.assertTrue("other should see fourth row", (count==4));


            // Delete
            pst2 = dbConn.prepareStatement("delete from "+TEST_TABLE+" ");
            pst2.executeUpdate();
            count = rowCount(dbConn, "afterTableClearing");
            Assert.assertTrue("make sure delete's lastInsertId did not corrupt connection protocol", (count==0) );

            count = rowCount(dbConn2, "other conn before commit");
            Assert.assertTrue("other should see four rows", (count==4));

            dbConn.commit();
            count = rowCount(dbConn, "afterTableClearing commit");
            Assert.assertTrue("make sure delete's lastInsertId did not corrupt connection protocol", (count==0) );
            count = rowCount(dbConn2, "other conn after delete");
            Assert.assertTrue("other should see nothing", (count==0));

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
