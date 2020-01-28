package com.paypal.integ.odak;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

import com.paypal.hera.dal.DalInit;
import com.paypal.hera.dal.DalInitException;
import com.paypal.hera.dal.DalRuntimeException;
import com.paypal.hera.dal.cal.CalConnectionWrapperFactory;
import com.paypal.hera.dal.cm.transaction.DalTransactionException;
import com.paypal.hera.dal.cm.transaction.DalTransactionManager;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerFactory;
import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;
import com.paypal.hera.dal.map.ConnectionManager;
import com.paypal.hera.dal.map.SQLOperationEnum;
import com.paypal.integ.odak.GroomerConfig;
import com.paypal.integ.odak.OdakConnection;
import com.paypal.integ.odak.OdakPool;
import com.paypal.integ.odak.OdakPoolManager;
import com.paypal.platform.security.PayPalSSLHelper;

/**
 * Tests cases regarding connection state. Can't put back closed in the pool.
 * 
 * 
 */
public class OCPTests {

	@Before
	public void init() throws Exception {
		PayPalSSLHelper.initializeSecurity();
		DalInit.init();
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		increaseTimeouts();
	}

	private void increaseTimeouts() throws NamingException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		pool.getConfig().setIdleTimeout(20000);
		pool.getConfig().setOrphanTimeout(50000);
		pool.getConfig().setForceRecycle(60000, 0, 0);
	}

	private void descreaseTimeouts() throws NamingException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		pool.getConfig().setIdleTimeout(1000);
		pool.getConfig().setOrphanTimeout(4000);
		pool.getConfig().setForceRecycle(6000, 0, 0);
	}

	@Test
	public void checkedOutButNotUsedConnTest() throws SQLException, NamingException, DalInitException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		Connection conn = pool.getPooledConnection();
		//pool.getConfig().setOrphanTimeout(10000000);
		//sleep(pool.getConfig().getOrphanTimeout(), true);
		sleep(10000, true);
		assertFalse("Connection shd be removed", OdakPoolManager.getInstance().getPool("xe").exists(conn));
	}

	// @Test
	// public void rollbackDirtyConn() throws DalInitException, SQLException,
	// NamingException {
	// OCPConnectionPool pool =
	// OCPConnectionPoolManager.getInstance().getPool("xe");
	// Connection conn = pool.getConnection();
	// PreparedStatement stmt = conn.prepareStatement("SELECT * FROM DUAL");
	// pool.getPoolConfig().setOrphanTimeout(10000);
	// sleep(pool.getPoolConfig().getOrphantimeout() + 2000);
	// //rs = stmt.executeQuery();
	// assertTrue("Connection shd not be removed",
	// OCPConnectionPoolManager.getInstance().getPool("xe").exists(conn));
	// }

	// Insert into EMP (EMPNO,ENAME,JOB,MGR,HIREDATE,SAL,COMM,DEPTNO) values
	// (109,'KING','PRESIDENT',null,to_date('17-NOV-81','DD-MON-RR'),5030,5,10);

	/**
	 * Don't use connection from OCPConnectionPoolManager. Use existing
	 * ConnectionManager so metadata fetch happens as part of
	 * ConnectionManager.getConnectionInternal
	 * 
	 * 
	 * If auto-commit is true, any exception should cause the rollback -
	 * dml/select.
	 * 
	 * @throws DalInitException
	 * @throws SQLException
	 * @throws NamingException
	 */
	@Test
	public void rollbackOnExceptionWithAutoCommit() throws DalInitException, SQLException, NamingException {
		// default auto-commit is enabled - if not in the transaction.
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		createEmp(conn, 59, "John", "Engineer", 10000);
		try {
			// explicitly marking dirty as know
			// exception will occur.
			ocpConn.setDirty(true);
			// unique constraint violation
			createEmp(conn, 59, "John", "Engineer", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Connection should not be dirty. Rollback shd have happened.", !ocpConn.isDirty());
		conn.close();
	}

	// in transaction - exception shd not cause rollback. App's responsibility
	// to roll back.
	@Test
	public void rollbackOnExceptionWithoutAutoCommit() throws DalInitException, SQLException, NamingException {
		// default auto-commit is enabled - if not in the transaction.
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		conn.setAutoCommit(false); // in-transaction behavior
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		createEmp(conn, 59, "John", "Engineer", 10000);
		try {
			// unique constraint violation
			createEmp(conn, 59, "John", "Engineer", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Connection should be dirty here.", ocpConn.isDirty());
		// Note that conn won't go back to the pool automatically as don't have
		// query engine.
		conn.close();
	}

	@Test
	public void rollbackDirtyCreateInTx() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		conn.setAutoCommit(false); // in-transaction behavior
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		assertTrue("Connection should be dirty here.", ocpConn.isDirty());
		conn.commit();
		assertTrue("Connection should not be dirty here.", !ocpConn.isDirty());
		createEmp(conn, 59, "John", "Engineer", 10000);
		assertTrue("Connection should be dirty here.", ocpConn.isDirty());
		conn.close();
		assertTrue("Connection should not be dirty here.", !ocpConn.isDirty());
	}

	@Test
	public void rollbackDirtyDeleteInTx() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		conn.setAutoCommit(false); // in-transaction behavior
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		conn.commit();
		createEmp(conn, 59, "John", "Engineer", 10000);
		conn.commit();
		deleteEmp(conn, 59);
		assertTrue("Connection should be dirty here.", ocpConn.isDirty());
		conn.close(); // close conn with dirty delete
		assertTrue("Connection should not be dirty here.", !ocpConn.isDirty());
	}

	@Test
	public void destroyOnStaleConnExpNotInTx() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		// wait for conn to be stale.
		try {
			Thread.sleep(6000); // occ server idle timeout shd be 5 sec
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			createEmp(conn, 59, "John", "Engineer", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Wrapper conn should be closed.", conn.isClosed());
		assertTrue("OCP conn should be closed.", ocpConn.isClosed());
	}

	@Test
	public void destroyOnStaleConnExpNotInTxSelect() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		createEmp(conn, 59, "John", "Engineer", 10000);
		// wait for conn to be stale.
		try {
			Thread.sleep(6000); // occ server idle timeout shd be 5 sec
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			EmployeeDO empDo = readEmp(conn, 59);
			assertNotNull(empDo);
			assertEquals(59, empDo.getEmpNo());
			assertEquals("Engineer", empDo.getJob());
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Wrapper conn should be closed.", conn.isClosed());
		assertTrue("OCP conn should be closed.", ocpConn.isClosed());
	}

	@Test
	public void destroyOnStaleConnExpNotInTxCreate() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);

		// wait for conn to be stale.
		try {
			Thread.sleep(6000); // occ server idle timeout shd be 5 sec
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			createEmp(conn, 59, "John", "Engineer", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Wrapper conn should be closed.", conn.isClosed());
		assertTrue("OCP conn should be closed.", ocpConn.isClosed());
	}

	@Test
	public void destroyOnStaleConnExpNotInTxDelete() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		createEmp(conn, 59, "John", "Engineer", 10000);
		// wait for conn to be stale.
		try {
			Thread.sleep(6000); // occ server idle timeout shd be 5 sec
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			deleteEmp(conn, 59);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertTrue("Wrapper conn should be closed.", conn.isClosed());
		assertTrue("OCP conn should be closed.", ocpConn.isClosed());
	}

	@Test
	public void destroyOnStaleConnExpInTx() throws DalInitException, SQLException, NamingException {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();
			// Both below flags are set when conn is retrieved as Tx is started.
			// ocpConn.setInTransaction(true);
			// ocpConn.setAutoCommit(false);

			Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
					SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
			OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);

			assertNotNull("ocp conn should not be null", ocpConn);
			deleteEmp(conn, 59);
			conn.commit();
			// wait for conn to be stale.
			try {
				Thread.sleep(6000); // occ server idle timeout shd be 5 sec
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				createEmp(conn, 59, "John", "Engineer", 10000);
			} catch (Exception e) {
				// End of Stream
				System.out.println("Starting the rollback");
				// on fatal errors, etc. conn is destroyed but remains open by
				// Tx mgr until rollback is called.
				dalTransMgr.rollback();
				System.out.println("rollback completed");
				e.printStackTrace();
			}

			assertTrue("OCP Connection should be closed.", ocpConn.isClosed());
			// Wrapper conn is not closed. You have to close the wrappers.
			assertTrue("Wrapper Connection should be closed.", conn.isClosed());
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	@Test
	public void returnUncmtedTxDirtyConnToPool()
			throws DalInitException, SQLException, NamingException, DalTransactionException {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		dalTransMgr.begin();

		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);

		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		conn.commit();
		createEmp(conn, 59, "John", "Engineer", 10000);
		// Keep conn dirty
		// conn.commit();
		conn.close();
		assertTrue("Wrapper Connection should not be closed.", !conn.isClosed());
		ocpConn.close();
		assertTrue("OCP Connection should not be closed.", !ocpConn.isClosed());
		ocpConn.destroyConnection();
		assertTrue("OCP Connection should be closed.", ocpConn.isClosed());
		assertTrue("Wrapper Connection should be closed.", conn.isClosed());
		// Wrapper conn is not closed. You have to close the wrappers.
	}

	@Test
	public void returnUncmtedTxCmtedConnToPool()
			throws DalInitException, SQLException, NamingException, DalTransactionException {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		dalTransMgr.begin();

		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);

		assertNotNull("ocp conn should not be null", ocpConn);
		deleteEmp(conn, 59);
		conn.commit();
		createEmp(conn, 59, "John", "Engineer", 10000);
		// conn.commit();
		conn.close();
		assertTrue("Wrapper Connection should not be closed.", !conn.isClosed());
		ocpConn.close();
		assertTrue("OCP Connection should not be closed.", !ocpConn.isClosed());
		ocpConn.destroyConnection();
		assertTrue("OCP Connection should be closed.", ocpConn.isClosed());
		assertTrue("Wrapper Connection should be closed.", conn.isClosed());
		// Wrapper conn is not closed. You have to close the wrappers.
	}

	@Test
	public void returnCmtedTxDirtyConnToPool() throws DalInitException, SQLException, NamingException {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();

			Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
					SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
			OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);

			assertNotNull("ocp conn should not be null", ocpConn);
			deleteEmp(conn, 59);
			conn.commit();
			createEmp(conn, 59, "John", "Engineer", 10000);
			// No explict commit so conn is dirty
			// conn.commit();

			// Tx commit calls commit on conn.
			dalTransMgr.commit();
			conn.close();

			// Wrapper closes itself but underlying OCP connection goes back to
			// the pool.
			assertTrue("Wrapper Connection should be closed.", conn.isClosed());
			assertTrue("OCP Connection should not be closed.", !ocpConn.isClosed());
			// TODO: Add method to test if it's in the pool. - check state of
			// conn as IDLE and shd be part of the free list.
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void returnCmtedTxCmtedConnToPool() throws DalInitException, SQLException, NamingException {
		DalTransactionManager dalTransMgr = DalTransactionManagerFactory.getDalTransactionManager();
		try {
			dalTransMgr.begin();

			Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
					SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
			OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);

			assertNotNull("ocp conn should not be null", ocpConn);
			deleteEmp(conn, 59);
			conn.commit();
			createEmp(conn, 59, "John", "Engineer", 10000);
			conn.commit();
			dalTransMgr.commit(); // this will make us out of transaction and
									// release conns.
			conn.close();

			// Wrapper closes itself but underlying OCP connection goes back to
			// the pool.
			assertTrue("Wrapper Connection should be closed.", conn.isClosed());
			assertTrue("OCP Connection should not be closed.", !ocpConn.isClosed());
			// TODO: Add method to test if it's in the pool. - check state of
			// conn as IDLE and shd be part of the free list.
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// test case:on any unique constaint violation connection shd go back to the
	// pool.
	// test case: on stale conn exception, conn shd be destroyed.
	@Test
	public void insertWithAutoCommitDisabled() throws DalInitException, SQLException, NamingException {
		Connection conn = ConnectionManager.getInstance().getConnection("xe", true, "sql", true, "xe",
				SQLOperationEnum.SQL_OP_INSERT, null, 0, -99);
		OdakConnection ocpConn = getOCPConnectionFromCalConnectionWrapper(conn);
		deleteEmp(conn, 59);
		ocpConn.setAutoCommit(false);
		deleteEmp(conn, 59);
		createEmp(conn, 59, "John", "Engineer", 10000);

		try {
			// will not throw unique constraint violation
			createEmp(conn, 59, "John", "Engineer", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		conn.commit();

		// try {
		// createEmp(conn, 59, "John", "Engineer", 10000);
		// } catch (Exception e) {
		// // unique const violated
		// System.out.println(e.getMessage());
		// }

		EmployeeDO empDo = readEmp(conn, 59);
		assertNotNull(empDo);
		assertEquals(59, empDo.getEmpNo());
		assertEquals("Engineer", empDo.getJob());
		assertNotNull("ocp conn should not be null", ocpConn);
		assertTrue("Connection should not be dirty", !ocpConn.isDirty());
	}

	public EmployeeDO readEmp(Connection conn, int empNo) throws SQLException {
		String selectSQL = "SELECT EMPNO, ENAME FROM EMP WHERE EMPNO = ?";
		PreparedStatement preparedStatement = conn.prepareStatement(selectSQL);
		preparedStatement.setInt(1, empNo);
		ResultSet rs = preparedStatement.executeQuery();
		EmployeeDO empDo = null;
		if (rs.next()) {
			int empNoFromDB = rs.getInt("EMPNO");
			String eName = rs.getString("ENAME");
			String job = rs.getString("JOB");
			int sal = rs.getInt("SAL");
			empDo = new EmployeeDO(empNoFromDB, eName, job, sal);
		}
		return empDo;
	}

	public void createEmp(Connection conn, int empNo, String empName, String job, int sal) throws SQLException {
		String insertSQL = "INSERT INTO EMP (EMPNO, ENAME, JOB, SAL, COMM,  MGR, HIREDATE) VALUES  (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement insertStmt = conn.prepareStatement(insertSQL);
		insertStmt.setInt(1, empNo);
		insertStmt.setString(2, empName);
		insertStmt.setString(3, job);
		insertStmt.setInt(4, sal);
		// insertStmt.setInt(5, 10);
		insertStmt.setInt(5, 5);
		insertStmt.setString(6, null);
		insertStmt.setDate(7, null);
		insertStmt.executeUpdate();
	}

	public void deleteEmp(Connection conn, int empNo) throws SQLException {
		String dml = "DELETE FROM EMP WHERE EMPNO = ?";
		PreparedStatement stmt = conn.prepareStatement(dml);
		stmt.setInt(1, 59);
		stmt.executeUpdate();
	}

	@Test
	public void checkedOutAndInUseConnection() throws DalInitException, SQLException, NamingException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		Connection conn = pool.getConnection();
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM DUAL");
		pool.getConfig().setOrphanTimeout(10000);
		sleep(pool.getConfig().getOrphanTimeout() + 2000);
		// rs = stmt.executeQuery();
		assertTrue("Connection shd not be removed", OdakPoolManager.getInstance().getPool("xe").exists(conn));
	}

	@Test
	public void idleConnTest() throws DalInitException, SQLException, NamingException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		Connection conn = pool.getPooledConnection();
		conn.close();
		sleep(pool.getConfig().getIdleTimeout(), true);
		assertFalse("Idle Connection shd be removed",
				OdakPoolManager.getInstance().getPool("xe").exists(conn));
	}

	@Test
	public void maxAgeConnTest() throws DalInitException, SQLException, NamingException {
		OdakPool pool = OdakPoolManager.getInstance().getPool("xe");
		Connection conn = pool.getPooledConnection();
		pool.getConfig().setSoftRecycle(500);
		pool.getConfig().setForceRecycle(1000, 0, 0);
		// make sure it does not get orphan
		pool.getConfig().setOrphanTimeout(30000);
		// PreparedStatement stmt = conn.prepareStatement("SELECT * FROM DUAL");

		sleep(pool.getConfig().getHardRecycle() + 1000);
		conn.close();
		// Don't wait for the next groom. It's removed while getting closed
		// (return back to pool).
		// sleepForGroomInterval();

		assertFalse("Maxage Connection shd be removed",
				OdakPoolManager.getInstance().getPool("xe").exists(conn));
		assertTrue("Connection shd be closed", conn.isClosed());
		// assert that it's destroyed
		// test from outside/func test that connection is cleanly destroyed.
	}

	public void sleep(long time, boolean adjustToGroomInterval) {
		int padding = 500;
		try {
			if (adjustToGroomInterval) {
				Thread.sleep(time + GroomerConfig.getInstance().getGroomInterval() + padding);
			} else {
				Thread.sleep(time);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void sleep(long time) {
		try {
			System.out.println("waiting for - " + time);
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void sleepForGroomInterval() {
		int padding = 500;
		try {
			Thread.sleep(GroomerConfig.getInstance().getGroomInterval() + padding);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public OdakConnection getOCPConnectionFromCalConnectionWrapper(Connection conn) {
		try {
			CmConnectionWrapper calConnectionWrap = (CmConnectionWrapper) CalConnectionWrapperFactory.getInstance()
					.unwrapConnection(conn);
			Connection poolConnn = CmConnectionWrapper.unwrap(calConnectionWrap);

			if (poolConnn == null) {
				throw new DalRuntimeException("OCP connection is null");
			}

			if (!(poolConnn instanceof OdakConnection)) {
				throw new DalRuntimeException("Not OCP connection");
			}
			return (OdakConnection) poolConnn;
		} catch (Exception ex) {
			throw new DalRuntimeException(ex.getMessage(), ex);
		}
	}

	private void testOCPPoolConfigValidation() {

	}

	static class EmployeeDO {
		int empNo;
		String name;
		String job;
		int sal;

		public EmployeeDO(int empNo, String name, String job, int sal) {
			this.empNo = empNo;
			this.name = name;
			this.job = job;
			this.sal = sal;
		}

		public int getEmpNo() {
			return empNo;
		}

		public void setEmpNo(int empNo) {
			this.empNo = empNo;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getJob() {
			return job;
		}

		public void setJob(String job) {
			this.job = job;
		}

		public int getSal() {
			return sal;
		}

		public void setSal(int sal) {
			this.sal = sal;
		}

	}
}
