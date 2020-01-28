package com.paypal.integ.odak;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.hera.dal.cm.wrapper.CmConnectionCallback;
import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

public class OdakAdapter implements CmConnectionCallback {
	private OdakPool pool;
	private PoolConfig ocpPoolConfig;

	private static final String UNSUPPORTED_OPERATION = "Not supported operation";

	private final static Logger logger = LoggerFactory.getLogger(OdakAdapter.class);

	public OdakAdapter() {
		//super(DalConstants.DRIVER_TYPE_SQL);
	}

	/**
	 * ConnectionPoolConfig is not used by OCP connection pool but to satisfy
	 * the existing flow and logging requirements by wrappers.
	 */
//	public void setInitialConfig(ConnectionPoolConfig poolConfig) throws NamingException {
//		//super.setInitialConfig(poolConfig);
//	}

	public void setOCPConfig(PoolConfig poolConfig) throws NamingException {
		this.ocpPoolConfig = poolConfig;
	}

	public void flushConnections() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);

	}

	public void flushConnection(Connection conn) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);

	}

	public void resetThrottle() {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);

	}

	public String getAutoFlushTypeDescription() {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

//	public Statement unwrapStatement(Statement origStmt) throws SQLException {
//		if (origStmt == null || pool == null) {
//			return origStmt;
//		}
//
//		Statement stmt = unwrapQeWrapper(origStmt);
//		stmt = CalConnectionWrapperFactory.getInstance().unwrapStatement(stmt);
//		stmt = pool.unwrapStatement(stmt);
//		return stmt;
//	}

//	public ConnectionPoolStats calculatePoolStats() {
//		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
//	}

	// Contract method
	public boolean isMaxedOut(SQLException sqle) {
		// SqlExceptionHelper: adapter.isMaxedOut(sqle) checks if wait time is
		// reached during execution.
		return false;
	}

	public boolean isConnectionTimeout(SQLException sqle) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public int getConnectRetryCountForIoException() {
		if (pool != null) {
			return pool.getConnectRetryCountForIoException();
		}

		return 0;
	}

	public void startup() throws NamingException {
		if (pool != null) {
			return;
		}
		pool = OdakPoolManager.getInstance().createPool(ocpPoolConfig.getHost(), this);
	}

	public void shutdown() throws SQLException {
		// This is not called from ConnectionManager's registered
		// custom pool adapter. Any shutdown sequence required for DAL. We don't
		// explicitly close conns and stops threads during shutdown.
	}

	
	public boolean isInitialized() {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

//	protected ResultSet unwrapResultSet(ResultSet origRs) throws SQLException {
//		return unwrapStdWrappers(origRs);
//	}

	@Override
	public void cmCallEnd(Connection connection, Statement stmt, JdbcOperationType opType, SQLException e) {
		/**
		 * DAL sets auto-commit but it's not database auto-commit. Means, if DML
		 * with auto-commit fails, then it may be commit failed but DML was
		 * performed. This is very confusing auto-commit elusion and can cause
		 * issues, if not understood. Usually, rollback is performed when
		 * auto-commit (Hikari and other pools) is false. But with DAL, we need
		 * to rollback if auto-commit is true.
		 * 
		 * In a nutshell, don't rollback for transaction. Otherwise, always
		 * rollback upon exception. Note that this is not when we put connection
		 * back to the pool. This is when we get exception.
		 * 
		 */
		OdakConnection conn = (OdakConnection) connection;
		//if (e != null && m_driverAdapter.needsRollbackAfterException(conn, opType, e)) {
			// make conn dirty on exception - whether auto-commit enabled or
			// not.
			conn.setDirty(true);
			/*
			 * Existing DCP code does not enable SI. So, keeping it consistent here
			 * and disabling. Enabling may break SI read timeout.
			 */
			// SocketInterceptHelper sih = new
			// SocketInterceptHelper(pool.getName(), null);
			try {
				if (conn.getAutoCommit()) {
					// sih.enableForUse();
					logger.info("Exception received. Not in transaction. Rolling back - conn: {},  exception: {} ",
							conn, e.getMessage());
					conn.rollback();
					conn.setDirty(false);
				}
			} catch (Throwable e2) {
				// getAutCommit on a closed conn throws exception as well.
				String msg = String.format(
						"Unable to rollback connection %s after an exception: %s. Exception received during rollback:%s. Connection will be closed without trying again.",
						conn, e, e2);
				logger.error("ODAK_CONN_ROLLBACK_FAILED - " + msg, e2);
				CalEventHelper.writeLog("ODAK_CONN_ROLLBACK_FAILED", conn.getPoolName(), msg, "0");
				conn.setDirty(false); // rollback won't be tried again.
				conn.destroyConnection();
			}
			// sih.disable();
		//}
		//super.cmCallEnd(conn, stmt, opType, e);
	}

	// contract method
	@Override
	public void cmConnectionClose(Connection conn) throws SQLException {
		// make sure not to close connection as it's closed
		// by the pool itself
	}

	// contract method
	@Override
	public void cmConnectionDestory(Connection conn) throws SQLException {
		// make sure not to close connection as it's destroyed
		// by the pool itself
	}
	
	public void cmCallStart(Connection conn, Statement stmt, JdbcOperationType opType) throws SQLException {
	}

	public SQLException cmProcessException(Connection conn, Statement stmt, JdbcOperationType opType, SQLException e) {
		return e;
	}

	@Override
	public boolean cmIsUtf8Db() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}



}
