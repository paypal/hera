package com.paypal.integ.odak;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerFactory;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerImpl;
import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;
import com.paypal.hera.jdbc.HeraConnection;

/**
 * 
 * Poolable connection for OCP pool.
 * 
 */
public class OdakConnection implements Connection {
	private Connection jdbcConn;
	private final OdakPool pool;
	private long createTime;
	private long lastUseTime;
	private long lastStmtExecutionTime;
	private AtomicLong inCallCount = new AtomicLong();
	private boolean inTransaction;
	private boolean isDirty;
	// newly built conn is in active state until parked in the free list.
	private AtomicInteger state = new AtomicInteger(State.ACTIVE.ordinal());
	private static final String UNSUPPORTED_OPERATION = "Not supported operation";
	private Object externalData;
	private String connUUID;
	private String occConnUUID;

	private final static Logger logger = LoggerFactory.getLogger(OdakConnection.class);

	public OdakConnection(Connection conn, OdakPool pool) {
		this.jdbcConn = conn;
		this.pool = pool;
		this.connUUID = genConnUUID();
		setCreateTime(System.currentTimeMillis());
		setLastUseTime(System.currentTimeMillis());
		if(conn instanceof HeraConnection){
			HeraConnection heraConnection = (HeraConnection) conn;
			try {
				this.occConnUUID = heraConnection.getClientInfo(HeraConnection.HERA_CLIENT_CONN_ID);
			} catch (SQLException e) {
				String msg = "getClientInfo failed. Occ connection id will not be populated";
				logger.info("ODAK_OCC_CONN_ID - {}", msg);
				CalEventHelper.writeLog("ODAK_OCC_CONN_ID", pool.getName(), msg, "0");
			}
		}
	}

	public String toString() {
		return String.format("[pool:%s, odakId:%s, connId:%s, state:%s, isDirty:%b]", pool.getName(), connUUID, occConnUUID,
				State.values()[state.get()], isDirty());
	}

	private String genConnUUID() {
		UUID uuid = UUID.randomUUID();
		return uuid.toString();
	}

	String getConnUUID() {
		return connUUID;
	}

	private enum State {
		IDLE, ACTIVE, AGED, CLOSED, DESTROYED
	};

	String getPoolName() {
		return pool.getName();
	}

	public void clearTransactionFlag() {
		inTransaction = false;
	}

	// contract methods from DCP
	public synchronized Object getExternalData() {
		return externalData;
	}

	public synchronized void setExternalData(Object value) {
		externalData = value;
	}

	/**
	 * 
	 * Verify: Existing code closes the wrappers after destroying the
	 * connection. Ideally, physical conn shd be destroyed as part of closing
	 * wrappers.
	 * 
	 * Check DcpConnectoinPool.destroyConnection. Here the actual JDBC
	 * connection is closed first via wasDestroyed = conn.destroyInternal(force,
	 * reason);. Then the actual wrapper is closed via closeWrapper() -
	 * ((CmConnectionWrapper)wrapper).close();
	 * 
	 * Check CmConnectionWrapper.close() --> It will not close the wrapper if
	 * we're in transaction. But the underlying connection is already destroyed.
	 * 
	 */
	@Override
	public void close() throws SQLException {
		if (isClosed()) {
			String msg = String.format("OCPPoolableConnection.close - Connection %s is already closed.", this);
			logger.info("ODAK_CONN_ALREADY_CLOSED - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ALREADY_CLOSED", pool.getName(), msg, "0");
			return;
		}
		setLastUseTime(System.currentTimeMillis());
		// don't return to pool if it's used in a transaction, and still active
		if (isInTransaction()) {
			DalTransactionManagerImpl dalTransMgr = (DalTransactionManagerImpl) DalTransactionManagerFactory
					.getDalTransactionManager();
			if (dalTransMgr.hasConnection(((CmConnectionWrapper) externalData))) {
				logger.debug(
						"OCPPoolableConnection.close - current thread is in transaction. Cannot return connection {} back to the pool {}",
						this, pool.getName());
				return;
			}
		}
		pool.returnConnection(this);
	}

	/**
	 * Destroys connections. Don't have to reset OCC hints. Rolls back if
	 * needed.
	 * 
	 * TODO: pass cause of destory for logging purpose.
	 */
	public void destroyConnection() {
		logger.debug("destroyConnection() - connection {} will be destroyed for pool: {}", this, pool.getName());
		if (isDestroyed()) {
			String msg = String.format("OCPPoolableConnection.destroyConnection: Connection %s is already destroyed",
					this);
			logger.info("ODAK_CONN_ALREADY_DESTROYED - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ALREADY_DESTROYED", pool.getName(), msg, "0");
			return;
		}
		// Irrespective of the current state, we can't use this
		// connection. Set to close and destroyed immediately so no further use
		// anywhere.
		setClosed();
		setDestroyed();
		/*
		 * Connection can be destroyed upon errors so it can be present in the
		 * active list. Should never be in the free list but just remove for
		 * sanity. Aged connections are destroyed while getting from free list
		 * or returning to the free list. Ideally, it should not be in any list
		 * while getting destroyed.
		 *
		 */
		pool.removeActiveConn(this);
		pool.removFreeConn(this);

		/*
		 * Verify: Do we need to remove from DAL/Hibernate transaction? Don't
		 * have to remove as it's transaction manager's responsibility to remove
		 * upon transaction commit/rollback or timeout. Keeping the behavior
		 * consistent with the existing DCP code.
		 */
		try {
			rollbackDirty();
			jdbcConn.close();
			logger.debug("destroyConnection() - connection {} is successfully destroyed for pool: {}", this,
					pool.getName());
			pool.incrDestroyedConns();
			pool.decrAllConnsByCreateDestory();
			closeWrapper(getExternalData());
		} catch (SQLException e) {
			String msg = String.format("OCPPoolableConnection.destroy failed. Conn: %s, Pool: %s", this,
					pool.getName());
			CalEventHelper.writeException("ODAK_DESTROY_CONN_FAILED", e, true, msg);
			logger.error("ODAK_DESTROY_CONN_FAILED" + msg, e);
		}
	}

	/**
	 * Invokes close on connection wrapper
	 */
	private void closeWrapper(Object wrapper) {
		if (isInTransaction()) {
			DalTransactionManagerImpl dalTransMgr = (DalTransactionManagerImpl) DalTransactionManagerFactory
					.getDalTransactionManager();
			if (dalTransMgr.hasConnection(((CmConnectionWrapper) externalData))) {
				/*
				 * Today, DAL does not remove it so should be ok. Need to expose
				 * method to remove destroyed conn from dalTransMgr, but why
				 * create dependency on it. Logging to measure the cases.
				 */
				String msg = String.format(
						"OCPPoolableConnection.destroy -  In transaction. Ideally, connection should be removed forcefully from the Tx as it is getting destoryed. Conn: %s, Pool: %s",
						this, pool.getName());
				logger.info("ODAK_TX_REMOVE_DESTROYED_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_TX_REMOVE_DESTROYED_CONN", pool.getName(), msg, "0");
			}
		}

		if (wrapper == null) {
			// There're cases when wrapper is not even created. For instance, if
			// connection is aged out while getting from the pool, the wrapper
			// is not created. Logging to measure all the cases.
			return;
		}

		try {
			((CmConnectionWrapper) wrapper).close();
		} catch (Throwable e) {
			String msg = String.format(
					"OCPPoolableConnection.destroy - closing connection wrapper failed. Conn: %s, Pool: %s", this,
					pool.getName());
			CalEventHelper.writeException("ODAK_WRAPPER_CLOSE_FAILED", e, true, msg);
			logger.error("ODAK_WRAPPER_CLOSE_FAILED - " + msg, e);
		}
	}

	public void rollbackDirty() {
		if (!isDirty()) {
			return;
		}
		// let the rollback happen if conn is marked closed but not yet
		// destroyed, or the destroy failed earlier for any reason.
		if (isDestroyed()) {
			String msg = String.format("OCPPoolableConnection.rollbackDirty: Connection %s is already destroyed", this);
			logger.info("ODAK_CONN_ALREADY_DESTROYED - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ALREADY_DESTROYED", pool.getName(), msg, "0");
			return;
		}

		try {
			rollback();
			String msg = String.format("Rolled back dirty connection %s for pool: %s", this, pool.getName());
			logger.info("ODAK_CONN_ROLLBACK - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ROLLBACK", pool.getName(), msg, "0");
			/**
			 * 
			 * DCP potential issue - verify: today, why set it to false even
			 * before doing rollback. What if rollback fails because of the IO
			 * exception or any reason? Check PoolableConnection.rollback().
			 * Rollback throws exception but transaction manager eats it and
			 * returns connection back to the pool.
			 */
			setDirty(false);

		} catch (Throwable e) {
			String msg = String.format(
					"Unable to rollback potentially dirty connection %s for pool: %s - error: %s. Connection will be closed without trying again.",
					this, pool.getName(), e);
			logger.error("ODAK_CONN_ROLLBACK_FAILED - " + msg, e);
			CalEventHelper.writeLog("ODAK_CONN_ROLLBACK_FAILED", pool.getName(), msg, "0");
			/**
			 * DCP potential issue - verify: Today we don't destroy connection
			 * if rollback fails. Check all the callers of
			 * PoolableConnection.rollback() including transaction manager.
			 */
			setDirty(false); // rollback won't be tried again.
			destroyConnection();
		}
	}

	public HeraConnection getHeraConnection() {
		if (jdbcConn instanceof HeraConnection) {
			return (HeraConnection) jdbcConn;
		}
		return null;
	}

	void setFirstSQL(boolean isFirstSQL) {
		if (jdbcConn instanceof HeraConnection) {
			((HeraConnection) jdbcConn).setFirstSQL(isFirstSQL);
		}
	}

	void resetOccHint() throws SQLException {
		if (this.jdbcConn instanceof HeraConnection) {
			((HeraConnection) this.jdbcConn).resetShardHints();
		}
	}

	boolean tryToSetActiveFromIdle() {
		return state.compareAndSet(State.IDLE.ordinal(), State.ACTIVE.ordinal());
	}

	/**
	 * Caller is expected to eventually destroy the connection after closing it.
	 * Don't expose outside the OCP pool.
	 * 
	 * @return
	 */
	boolean tryToSetClosedFromIdle() {
		boolean result = state.compareAndSet(State.IDLE.ordinal(), State.CLOSED.ordinal());
		if (result) {
			pool.incrIdleClosed();
		}
		return result;
	}

	boolean tryToSetIdleFromActive() {
		return state.compareAndSet(State.ACTIVE.ordinal(), State.IDLE.ordinal());
	}

	boolean isInTransaction() {
		return inTransaction;
	}

	void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	boolean isDirty() {
		return isDirty;
	}

	void setDirty(boolean isDirty) {
		this.isDirty = isDirty;
	}

	private void setClosed() {
		if (state.get() == State.CLOSED.ordinal() || state.get() == State.DESTROYED.ordinal()) {
			return;
		}
		if (state.compareAndSet(State.IDLE.ordinal(), State.CLOSED.ordinal())) {
			pool.incrClosedConns();
			pool.incrIdleClosed();
		} else if (state.compareAndSet(State.ACTIVE.ordinal(), State.CLOSED.ordinal())) {
			pool.incrClosedConns();
			pool.incrActiveClosed();
		} else if (state.compareAndSet(State.AGED.ordinal(), State.CLOSED.ordinal())) {
			pool.incrClosedConns();
			pool.incrAgedClosed();
		} else {
			String msg = String.format(
					"OCPPoolableConnection.setClosed: Closing connection %s which is not idle, active or aged", this);
			logger.info("ODAK_CONN_CLOSE_UNKNOWN_STATE - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_CLOSE_UNKNOWN_STATE", pool.getName(), msg, "0");
			state.set(State.CLOSED.ordinal());
			pool.incrClosedConns();
		}
	}

	void setAged() {
		if (state.get() == State.CLOSED.ordinal() || state.get() == State.DESTROYED.ordinal()) {
			return;
		}
		if (state.compareAndSet(State.IDLE.ordinal(), State.AGED.ordinal())
				|| state.compareAndSet(State.ACTIVE.ordinal(), State.AGED.ordinal())) {
			pool.incrAgedConnsCount();
		} else {
			String msg = String.format("OCPPoolableConnection.setAged: Aging connection %s which is not idle or active",
					this);
			logger.info("ODAK_CONN_AGE_UNKNOWN_STATE - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_AGE_UNKNOWN_STATE", pool.getName(), msg, "0");
			state.set(State.AGED.ordinal());
			pool.incrAgedConnsCount();
		}
	}

	private void setDestroyed() {
		if (state.get() == State.DESTROYED.ordinal()) {
			return;
		}
		setClosed();
		state.set(State.DESTROYED.ordinal());
	}

	/**
	 * Upper layer (DAL ORM/ Query engine) uses this to know if connection is
	 * closed/destroy or not. It does not matter if it's marked as closed or
	 * physically destroyed for the upper layer. If it's just marked closed, it
	 * will also be eventually destroyed - as part of the destroy, rollback will
	 * also happen.
	 * @throws SQLException 
	 */
	@Override
	public boolean isClosed() throws SQLException {
		/*
		 * Even if underlying occ connection is open, OCP can decide to close
		 * its poolable connection and eventually destroy underlying occ
		 * connection. If occ connection is closed upon error or any other
		 * reason, OCP poolable connection will go to close and then destroyed
		 * state immidiately as part of flush during exception handling.
		 */
		if (jdbcConn == null || state.get() == State.CLOSED.ordinal() || state.get() == State.DESTROYED.ordinal()) {
			return true;
		}
		if (jdbcConn.isClosed()) {
			String msg = String
					.format("OCPPoolableConnection.isClosed: Underlying occ connection is already closed by occ-jdbc driver. "
							+ "ocp conn:%s. Closing OCP poolable connection.", this);
			logger.info("ODAK_UNDERLYING_CONN_ALREADY_CLOSED - {}", msg);
			CalEventHelper.writeLog("ODAK_UNDERLYING_CONN_ALREADY_CLOSED", pool.getName(), msg, "0");
			// Don't just alter state to DESTROYED, but actually destroy so the
			// proper clean up happens and we don't introduce yet another flow.
			destroyConnection();
			return true;
		}
		return false;
	}

	/**
	 * This method should never be exposed outside OCP. Code outside OCP shd
	 * only use isClosed()
	 * 
	 * @return
	 */
	private boolean isDestroyed() {
		if (jdbcConn == null || state.get() == State.DESTROYED.ordinal()) {
			return true;
		}
		return false;
	}

	@Override
	public void commit() throws SQLException {
		jdbcConn.commit();
		/*
		 * If not set, unnecessary rollback can happen when connection is
		 * returned to the pool and autocommit is not enabled (means in
		 * transaction)
		 */
		setDirty(false);
	}

	@Override
	public void rollback() throws SQLException {
		logger.debug("OCPPoolableConnection:Rollback() {}" +
				jdbcConn.getClientInfo(HeraConnection.HERA_CLIENT_CONN_ID));
		jdbcConn.rollback();
		/*
		 * If rollback is called from app code, we don't want to set dirty flag
		 * to false. Only successful commit will set it to false. Even if the
		 * last call is rollback, we still try to make sure to rollback before
		 * putting back to the pool, as we don't know if rollback executed by app
		 * was successful or not. If we can't rollback while putting back to the
		 * pool, conn will be destroyed.
		 */
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return jdbcConn.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Statement createStatement() throws SQLException {
		Statement statement = jdbcConn.createStatement();
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement pStatement = jdbcConn.prepareStatement(sql);
		return pStatement;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		CallableStatement cStatement = jdbcConn.prepareCall(sql);
		return cStatement;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		Statement statement = jdbcConn.createStatement(resultSetType, resultSetConcurrency);
		return statement;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		PreparedStatement pStatement = jdbcConn.prepareStatement(sql, resultSetType, resultSetConcurrency);
		return pStatement;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return jdbcConn.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		PreparedStatement pStatement = jdbcConn.prepareStatement(sql, autoGeneratedKeys);
		return pStatement;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return jdbcConn.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		jdbcConn.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return jdbcConn.getAutoCommit();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return jdbcConn.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		jdbcConn.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return jdbcConn.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		jdbcConn.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return jdbcConn.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		jdbcConn.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return jdbcConn.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return jdbcConn.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		jdbcConn.clearWarnings();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return jdbcConn.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		jdbcConn.setTypeMap(map);

	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);

	}

	@Override
	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return getHeraConnection().getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public String getSchema() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public long getCreateTime() {
		return createTime;
	}

	private void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public long getLastUseTime() {
		return lastUseTime;
	}

	void setLastUseTime(long lastUseTime) {
		this.lastUseTime = lastUseTime;
	}

	public long getLastStmtExecutionTime() {
		return lastStmtExecutionTime;
	}

	public void setLastStmtExecutionTime(long lastStmtExecutionTime) {
		this.lastStmtExecutionTime = lastStmtExecutionTime;
	}

	public long getInCallCount() {
		return inCallCount.get();
	}

	public void incrInCallCount() {
		inCallCount.incrementAndGet();
	}

	public void decrInCallCount() {
		inCallCount.decrementAndGet();
	}
}
