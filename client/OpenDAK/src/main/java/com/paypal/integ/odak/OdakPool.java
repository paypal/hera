package com.paypal.integ.odak;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.naming.NamingException;

import com.paypal.hera.client.HeraClient;
import com.paypal.hera.jdbc.HeraConnection;
import com.paypal.hera.jdbc.HeraDriver;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerFactory;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerImpl;
import com.paypal.hera.dal.cm.wrapper.CmConnectionCallback;
import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapter;
import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;
import com.paypal.integ.odak.exp.OdakDataStoreConnectException;
import com.paypal.integ.odak.exp.OdakMaxConnectionException;

public class OdakPool {

	private PoolConfig config;
	private String host;
	private int size; // 0 + extra capacity as initial size
	private JdbcDriverAdapter driverAdapter;
	private CmConnectionCallback cmProxyCallbakUplink;	
	private CmProxyCallback m_cmProxyCallbak = new CmProxyCallback();
	private Queue<OdakConnection> freeConns = new ConcurrentLinkedQueue<>();
	private Queue<OdakConnection> activeConns = new ConcurrentLinkedQueue<>();
	private Map<String, Driver> drivers = new ConcurrentHashMap<>();
	private Queue<Integer> spikes = new ConcurrentLinkedQueue<>();

	// TODO: Generalize counters in the next release
	private AtomicInteger inFlightNewConn = new AtomicInteger(0);
	private AtomicInteger allConnsByCreateDestory = new AtomicInteger(0);
	private AtomicLong idleConnsCount = new AtomicLong(0);
	private AtomicLong agedConnsCount = new AtomicLong(0);
	private AtomicLong orphanConnsCount = new AtomicLong(0);
	private AtomicLong fgConnsCreated = new AtomicLong(0);
	private AtomicLong bkgConnsCreated = new AtomicLong(0);
	private AtomicLong bkgConnsReqs = new AtomicLong(0);
	private AtomicLong poolResizeBkgConnsReqs = new AtomicLong(0);
	private AtomicLong destroyedConns = new AtomicLong(0);
	private AtomicLong closedConns = new AtomicLong(0);
	private AtomicLong idleClosed = new AtomicLong(0);
	private AtomicLong activeClosed = new AtomicLong(0);
	private AtomicLong agedClosed = new AtomicLong(0);
	private static final int MAX_CONNECT_ATTEMPTS = 4;
	private final static Logger logger = LoggerFactory.getLogger(OdakPool.class);
	// private ReentrantLock lock = new ReentrantLock();
	private long lastSpikeAdj = System.currentTimeMillis();
	private static final int SPIKE_ADJ_IN_PROGRESS = 0;
	private static final int SPIKE_ADJ_DONE = 1;
	private AtomicInteger spikeLock = new AtomicInteger(SPIKE_ADJ_DONE);
	
	public OdakPool(String host, PoolConfig config, JdbcDriverAdapter driverAdapter,
			CmConnectionCallback cmProxyCallbakUplink) {
		this.host = host;
		this.config = config;
		this.driverAdapter = driverAdapter;
		this.cmProxyCallbakUplink = cmProxyCallbakUplink;
		// this.existingConfig = existingConfig;
	}

	/**
	 * Find driver to connect to database for a given url.
	 * 
	 * Wait-free and optimized than the DCP findDriver. This may also help
	 * reducing new connection creation time. TODO: test improvement.
	 * 
	 * @param url
	 * @return
	 * @throws SQLException
	 */
	private Driver findDriver(String url) throws SQLException {
		Driver driver = drivers.get(url);
		if (driver == null) {
			driver = DriverManager.getDriver(url);
			Driver existingDriver = drivers.putIfAbsent(url, driver);
			if (existingDriver != null) {
				// other thread already initialized.
				return existingDriver;
			}
		}
		return driver;
	}

	public Connection getConnection() throws SQLException {
		return getConnection(null);
	}

	/**
	 * 
	 * Contract method
	 * 
	 * @param dbSessionParams
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(HashMap dbSessionParams) throws SQLException {
		CmConnectionWrapper cmConnWrap = null;
		DalTransactionManagerImpl dalTransMgr = (DalTransactionManagerImpl) DalTransactionManagerFactory
				.getDalTransactionManager();
		boolean inTransaction = dalTransMgr.isInTransaction();
		if (inTransaction) {
			// Current thread is in transaction
			logger.debug(
					"Pool name: {}. In transaction. Return connection from dalTransMgr. If not found, get it from the OCP pool. If the pool is empty, create new.",
					getName());
			cmConnWrap = (CmConnectionWrapper) dalTransMgr.getConnection(getName());
		}

		/*
		 * Connection can be closed because of any exception and not yet removed
		 * from the dal transaction manager. DCP potential issue - this case is
		 * not considered today.
		 */
		if (cmConnWrap == null || cmConnWrap.isClosed()) {
			OdakConnection poolConn = getPooledConnection();
			cmConnWrap = setAndWrapConnection(poolConn, dbSessionParams, inTransaction);
		}
		return cmConnWrap;
	}

	/**
	 * Get connection from the pool. Wait-free and optimized.
	 * 
	 * Multiple threads can ask for a connection at the same time. Any future
	 * modification to this method should be done such a way that no concurrent
	 * situation is introduced.
	 * 
	 * @return
	 * @throws SQLException
	 * @throws NamingException
	 */
	public OdakConnection getPooledConnection() throws SQLException {
		OdakConnection conn = getFirstFreeConn();
		// defensive check to make sure connection is not closed for any reason.
		// connection is never placed in free list if any problem.
		if (conn != null && !conn.isClosed()) {
			// check as other thread can get the connection meantime and pool is
			// empty. Do not rely on pool empty check as it can be dequeued
			// concurrently afterwards.
			if (conn.tryToSetActiveFromIdle()) {
				// may not be idle anymore if in-process of getting closed.
				logger.debug("Getting connection from the OCP pool {} {}", getName(),
						conn.getClientInfo(HeraConnection.HERA_CLIENT_CONN_ID));
				addActiveConn(conn); // not have to be atomic with poll
				return conn;
			} else {
				String msg = String.format(
						"Cannot convert connection to active state from the idle. New conn will be created. Pool: %s, Conn:%s",
						getName(), conn);
				logger.info("ODAK_CONN_REUSE_FAILED - {}", msg);
				CalEventHelper.writeLog("ODAK_CONN_REUSE_FAILED", getName(), msg, "0");
				// This case should only happen when connection is closed by
				// groomer. We can ignore this connection as already removed
				// from free/active list and closed by the groomer. This is
				// extremely rear case as busy pool won't have idle connections
				// because of FIFO manner of the pool, so no need to retry to
				// get from the pool. Just create a new conn.
			}
		}
		Connection occConn = createNewConnection(false);
		conn = new OdakConnection(occConn, this);
		// lock.lock();
		addActiveConn(conn);
		inFlightNewConn.decrementAndGet();
		// lock.unlock();
		return conn;
	}

	/**
	 * First free conn from the pool. Takes care of aged conn during checkout to
	 * be more accurate on expiration. Today it's taken care only during the
	 * return to the pool and hence we get frequent late aging leading to stale
	 * connection, especially during occ rollouts.
	 * 
	 * @return null when pool is empty.
	 */
	private OdakConnection getFirstFreeConn() {
		while (!freeConns.isEmpty()) {
			OdakConnection conn = freeConns.poll();
			// double check as no global lock.
			if (conn == null) {
				return null;
			}
			if (tryRemoveAgedConn(conn)) {
				String msg = String.format("pool=%s&conn=%s",
						getName(), conn);
				logger.info("ODAK_AGED_REMOVED_ON_CHECKOUT - {}", msg);
				CalEventHelper.writeLog("ODAK_AGED_REMOVED_ON_CHECKOUT", getName(), msg, "0");
			} else {
				return conn;
			}
		}
		int activeCnt = getActiveConnsCount();
		long spikeAdjTimeElapsed = System.currentTimeMillis() - lastSpikeAdj;
		if (activeCnt > 0 && activeCnt >= getSize()) {
			spikes.add(activeCnt);
			if (spikeAdjTimeElapsed > GroomerConfig.getInstance().getSpikeAdjInterval()) {
				doSpikeAdjustment(activeCnt, spikeAdjTimeElapsed);
			}
		}
		return null;
	}
	
	
	private void doSpikeAdjustment(int activeCnt, long spikeAdjTimeElapsed) {
		int currSize = getSize();
		int newSize = currSize + config.getPoolExtraCapacity();
		if (spikeLock.compareAndSet(SPIKE_ADJ_DONE, SPIKE_ADJ_IN_PROGRESS)) {
			long newSpikeAdjTimeElapsed = System.currentTimeMillis() - lastSpikeAdj;
			if (newSpikeAdjTimeElapsed > GroomerConfig.getInstance().getSpikeAdjInterval()) {
				lastSpikeAdj = System.currentTimeMillis();
				setSize(newSize);
				OdakSizeAdjuster.getInstance().addCapacity(this, newSize, true);
				String msg = String.format(
						"pool=%s&newSize=%d&currSize=%d&activeCnt=%d&spikes=%s&spikeAdjTimeElapsed=%d",
						getName(), newSize, currSize, activeCnt, Arrays.toString(getSpikes()), spikeAdjTimeElapsed);
				logger.info("ODAK_ADJ_SPIKE - {}", msg);
				CalEventHelper.writeLog("ODAK_ADJ_SPIKE", getName(), msg, "0");
			} else {
				String msg = String.format(
						"Another spike adjustment just occurred. This will be skipped - pool:%s, newSize:%d, currSize:%d, activeCnt:%d, spikes:%s, "
								+ "spikeAdjTimeElapsed:%d, newSpikeAdjTimeElapsed:%d",
						getName(), newSize, getSize(), activeCnt, Arrays.toString(getSpikes()), spikeAdjTimeElapsed,
						newSpikeAdjTimeElapsed);
				logger.info("ODAK_ADJ_SPIKE_SKIPPED - {}", msg);
				CalEventHelper.writeLog("ODAK_ADJ_SPIKE_SKIPPED", getName(), msg, "0");
			}
			spikeLock.set(SPIKE_ADJ_DONE);
		} else {
			String msg = String.format(
					"Another spike adjustment is already in progress. This will be skipped - pool:%s, newSize:%d, currSize:%d, activeCnt:%d, spikes:%s, spikeAdjTimeElapsed:%d",
					getName(), newSize, getSize(), activeCnt, Arrays.toString(getSpikes()), spikeAdjTimeElapsed);
			logger.info("ODAK_ADJ_SPIKE_SKIPPED - {}", msg);
			CalEventHelper.writeLog("ODAK_ADJ_SPIKE_SKIPPED", getName(), msg, "0");
		}
	}

	/**
	 * Establishes connection using driver. Never returns null connections.
	 * Either valid conn or exception is returned.
	 * 
	 * 
	 * @param isBackground
	 * @return
	 * @throws SQLException
	 * @throws NamingException
	 * @throws OdakDataStoreConnectException
	 */
	private Connection createNewConnection(boolean isBackground) throws SQLException {
		// lock.lock();
		inFlightNewConn.incrementAndGet();
		if (getCurrentConnsCount() + inFlightNewConn.get() > config.getMaxConnections()) {
			String msg = String.format(
					"Max capacity reached for the pool. Usually, this is a very high default limit, and does not need be configured. Reach out to DAL team if you hit. "
							+ "pool: %s. maxConns: %d, currentConns: %d, inFlightNewConn: %d isBkg:%b, state:%s",
					getName(), config.getMaxConnections(), getCurrentConnsCount(), inFlightNewConn.intValue(),
					isBackground, StateLogger.getInstance().getState(getName()));
			inFlightNewConn.decrementAndGet();
			CalEventHelper.writeLog("ODAK_MAX_CONN_REACHED", getName(), msg, "1");
			throw new OdakMaxConnectionException(msg);
		}
		// lock.unlock();
		String msg = String.format("Creating a new connection - pool:%s, isBkg:%b, state:%s.", getName(),
				isBackground, StateLogger.getInstance().getState(getName()));
		String eventType = isBackground ? "ODAK_BKG_CONNECT" : "ODAK_FG_CONNECT";
		logger.info("{} - {}", eventType, msg);
		CalEventHelper.writeLog(eventType, getName(), msg, "0");

		Properties addProperties = new Properties();
		addProperties.put("dsName", host);
		addProperties.put("fg", isBackground?"0":"1");
		
		//TODO:HERA
		//Properties connectionProps = existingConfig.getConnectionProperties();
		Properties connectionProps = config.getConnectionProperties();
		if (addProperties != null) {
			connectionProps.putAll(addProperties);
		}
		
		//String url = existingConfig.getJdbcURL();
		String url = config.getUrl();
		Driver driver = findDriver(url);
		Connection conn;
		try {
			conn = driver.connect(url, connectionProps);
		} catch (SQLException sqle) {
			msg = String.format("Cannot connect to data source: %s. isBkg:%b, state:%s", getName(),
					isBackground, StateLogger.getInstance().getState(getName()));
			eventType = isBackground ? "ODAK_BKG_CONNECT_FAILED" : "ODAK_FG_CONNECT_FAILED";
			CalEventHelper.writeException(eventType, sqle, true, msg);
			inFlightNewConn.decrementAndGet();
			logger.error(eventType + " - " + msg, sqle);
			throw new OdakDataStoreConnectException(msg, sqle);
		}
		if (conn == null) {
			inFlightNewConn.decrementAndGet();
			msg = String.format(
					"Driver used is not right. Connection returned is null from driver's connect. This happens when "
							+ "incorrect driver is used to connect to db. Pool: %s, isBkg: %b",
					getName(), isBackground);
			eventType = isBackground ? "ODAK_BKG_CONNECT_FAILED" : "ODAK_FG_CONNECT_FAILED";
			CalEventHelper.writeLog(eventType, getName(), msg, "1");
			logger.error(eventType + " - " + msg);
			throw new OdakDataStoreConnectException(msg);
		}
		incrAllConnsByCreateDestory();
		if (isBackground) {
			bkgConnsCreated.getAndIncrement();
		} else {
			fgConnsCreated.getAndIncrement();
		}
		return conn;
	}

	void processConnectRequest() throws SQLException, NamingException {
		Connection occConn = createNewConnection(true);
		OdakConnection poolableConn = new OdakConnection(occConn, this);
		// lock.lock();
		addFreeConn(poolableConn);
		inFlightNewConn.decrementAndGet();
		// lock.unlock();
	}

	// Contract method
	private CmConnectionWrapper setAndWrapConnection(OdakConnection conn, HashMap dbSessionParams,
			boolean inTransaction) throws SQLException {
		CmConnectionWrapper cmConnWrap = null;
		conn.setLastUseTime(System.currentTimeMillis());
		conn.setFirstSQL(true);
		try {
			cmConnWrap = wrapConnection(conn, dbSessionParams, inTransaction);
			conn.setAutoCommit(!inTransaction);

			if (driverAdapter.supportsTransactionIsolation()) {
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			}
			conn.setInTransaction(inTransaction);
			if (inTransaction) {
				DalTransactionManagerImpl dalTransMgr = (DalTransactionManagerImpl) DalTransactionManagerFactory
						.getDalTransactionManager();
				dalTransMgr.addConnectionForTransaction(getName(), cmConnWrap);
			}
		} catch (RuntimeException | Error e) {
			String msg = String.format("Cannot create connection wrapper. pool: %s, conn: %s", getName(), conn);
			CalEventHelper.writeException("ODAK_WRAPPER_CREATE_FAILED", e, true, msg);
			logger.error("ODAK_WRAPPER_CREATE_FAILED - " + msg, e);
			conn.destroyConnection();
			throw e;
		}
		return cmConnWrap;
	}

	private CmConnectionWrapper wrapConnection(OdakConnection conn, HashMap dbSessionParams,
			boolean inTransaction) throws SQLException {
		CmConnectionWrapper wrapper = null;
		try {
			wrapper = new CmConnectionWrapper(conn, m_cmProxyCallbak, driverAdapter, dbSessionParams, inTransaction);
			conn.setExternalData(wrapper);
			wrapper.setSessionParameters();
		} catch (SQLException e) {
			throw m_cmProxyCallbak.cmProcessException(conn, null, JdbcOperationType.CONN_MISC, e);
		} catch (RuntimeException e) {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (Throwable sqe) {
					String msg = String.format("OCPConnectionPool.wrapConnection: Unable to close CmConnectionWrapper for pool: %s, detail: %s",
							getName(), e.getMessage());
					logger.error("ODAK_WRAPPER_CLOSE_FAILED - " + msg, sqe);
					CalEventHelper.writeLog("ODAK_WRAPPER_CLOSE_FAILED", getName(), msg, "0");
				}
			}
			throw e;
		}
		return wrapper;
	}

	/**
	 * Returns connection back to this pool. Wait-free and optimized. Multiple
	 * threads can return connection at the same time. Any future modification
	 * to this method should be done such a way that no concurrent situation is
	 * introduced.
	 * 
	 * @param conn
	 */
	void returnConnection(OdakConnection conn) throws SQLException{
		logger.debug("Returning connection to pool {} {}", getName(),
				conn.getClientInfo(HeraConnection.HERA_CLIENT_CONN_ID));
		conn.rollbackDirty();
		removeActiveConn(conn);
		/*
		 * Remove aged connection while returning, otherwise connection can be
		 * checked out from the pool within grooming time, and can never get
		 * removed.
		 */
		if (tryRemoveAgedConn(conn)) {
			logger.info("Aged out connection discarded while returning to the Pool: {}, Connection: {}", getName(),
					conn);
		} else {
			try {
				conn.resetOccHint();
			} catch (SQLException e) {
				String msg = String.format(
						"Cannot reset occ hints while returning connection %s back to the pool %s. Conn will be destroyed. Error received: %s",
						conn, getName(), e);
				logger.error("ODAK_RESET_OCC_HINT_FAILED - " + msg, e);
				CalEventHelper.writeException("ODAK_RESET_OCC_HINT_FAILED", e, true, msg);
				conn.destroyConnection();
				return;
			}

			conn.clearTransactionFlag();
			// Failed soft removal or not aged
			addFreeConn(conn);
		}
	}

	private boolean tryRemoveAgedConn(OdakConnection conn) {
		long age = System.currentTimeMillis() - conn.getCreateTime();
		if (age >= config.getSoftRecycle()) {
			long hardRecycle = config.getHardRecycle();
			if (age < hardRecycle) {
				if (freeConns.size() < config.getPoolExtraCapacityForAging()) {
					logger.info(
							"Aged out connection removal will be re-tried soon once have enough free conns - pool:{}, age:{}, hardRecycle:{}, conn:{}",
							getName(), age, hardRecycle, conn);
					/*
					 * If we recycle now, it will impact foreground traffic as
					 * not enough free conns. We may see spike in foreground
					 * creations as well as we may end up with more conns until
					 * next aging interval
					 */
					return false;
				}
			} else {
				String msg = String.format(
						"Hard recycle of aged connection - pool:%s, age:%d, hardRecycle:%d, conn:%s, isInTransaction:%b, inCallCount:%d, state:%s",
						getName(), age, hardRecycle, conn, conn.isInTransaction(), conn.getInCallCount(),
						StateLogger.getInstance().getState(getName()));
				logger.info("ODAK_AGED_HARD_RECYCLE - {}", msg);
				CalEventHelper.writeLog("ODAK_AGED_HARD_RECYCLE", getName(), msg, "0");
			}
			conn.setAged();
			conn.destroyConnection();
			OdakGroomer.getInstance().addConnectionRequest(this, true);
			return true;
		}
		return false;
	}

	/**
	 * Do not remove aged connections from the free list. Remove only when
	 * checked out or returned back to the pool. Returned conn may just stay for
	 * a moment before an another requests checks it out.
	 * 
	 */
	void removeIdleConnection() {
		for (OdakConnection conn : freeConns) {
			long idleTime = System.currentTimeMillis() - conn.getLastUseTime();
			if (idleTime >= config.getIdleTimeout()) {
				if (!conn.tryToSetClosedFromIdle()) {
					// Skip as state is not idle anymore. Can be in process of
					// getting checked out.
					continue;
				}
				incrClosedConns();
				removeIdleConn(conn);
				logger.info("Idle connection removed - Pool: {}, IdleTime: {}, Connection: {}", getName(), idleTime,
						conn);
				OdakGroomer.getInstance().addConnectionRequest(this, true);
			}
		}
	}

	void removeOrphanConnections() {
		for (OdakConnection conn : activeConns) {
			/*
			 * Change in behavior - Consider connection as orphan as long as it
			 * is checked out for a long time - even in transaction. Today,
			 * application can keep very high transaction timeout and hold
			 * connections for very long time causing max connections and other
			 * issues in production.
			 */
			// if (conn.getInCallCount() != 0 || conn.isInTransaction()) {
			// return;
			// }
			long orphanTime = System.currentTimeMillis() - conn.getLastUseTime();
			if (orphanTime >= config.getOrphanReport()) {
				String msg = String.format(
						"Connections remain checked out by application for a long time. DAL will consider them orphan and start destroying at some point."
								+ "If your application uses long running transactions, please reach out to the DAL or DBA team "
								+ "on how to break into multiple small running transactions. "
								+ "pool %s, orphanTime:%d, orphanReportTime:%d, Conn=%s, isInTransaction:%b, inCallCount:%d, state:%s",
						getName(), orphanTime, config.getOrphanReport(), conn, conn.isInTransaction(),
						conn.getInCallCount(), StateLogger.getInstance().getState(getName()));
				logger.info("ODAK_ORPHAN_CONN_DETECTED - {}", msg);
				CalEventHelper.writeLog("ODAK_ORPHAN_CONN_DETECTED", getName(), msg, "0");
			}
			if (orphanTime >= config.getOrphanTimeout()) {
				String msg = String.format(
						"pool=%s&orphanTime=%d&orphanTimeout=%d&Conn=%s&isInTransaction=%b&inCallCount=%d&state=%s",
						getName(), orphanTime, config.getOrphanTimeout(), conn, conn.isInTransaction(),
						conn.getInCallCount(), StateLogger.getInstance().getState(getName()));
				logger.info("ODAK_ORPHAN_CONN_DESTROY - {}", msg);
				CalEventHelper.writeLog("ODAK_ORPHAN_CONN_DESTROY", getName(), msg, "0");

				removeOrphanConn(conn);
			}
		}
	}

	// Contract method
	private class CmProxyCallback implements CmConnectionCallback {
		public void cmCallStart(Connection conn, Statement stmt, JdbcOperationType opType) throws SQLException {
			if (conn == null) {
				// Upper layer DAL code makes conn null randomly on some
				// exceptions. Logging to measure these cases.
				String msg = "cmCallStart - conn is null";
				logger.info("ODAK_CMCALL_NULL_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_CMCALL_NULL_CONN", getName(), msg, "0");
				return;
			}
			OdakConnection ocpConn = (OdakConnection) conn;
			ocpConn.incrInCallCount();

			/*
			 * Options for rollback when connection is put back in the pool:
			 * Note that upon exception, we rollback as part of
			 * OCPConnectionPoolAdapter.cmCallEnd which is a different scenario
			 * than rolling back while putting back to the pool
			 * 
			 * Option-1: Always rollback before putting back to the pool, if
			 * last statement executed is dml. - potential latency increase from
			 * dml. If auto-commit is enabled, rollback is redundant. Option-2:
			 * Rollback only if auto-commit is not enabled (connection was used
			 * in transaction). What if commit fails after successful dml.
			 * verify: review existing dcp implementation.
			 * 
			 * DCP implements option-2 but may not be safe as when auto-commit
			 * is enabled, OCC client sends DML and Commit msg separately - what
			 * if commit fails. OCPConnectionPoolAdapter.cmCallEnd rolls back
			 * upon certain exception if auto-commit is enabled but during
			 * returning connection back to the pool, there is no check again.
			 * Safest option is 1, and If latency becomes concern, we can do
			 * rollback in background after sending user response. Currently
			 * keeping the option-2 similar to DCP.
			 * 
			 * In nutshell - for DAL, this is mainly for the Tx cases where
			 * auto-commit is not enabled. Tx shd be committed or rolledback
			 * before putting to the pool. If forgotten, we can detect via dirty
			 * flag and rollback.
			 */
			if (!ocpConn.getAutoCommit() && opType.isDmlOperation()) {
				ocpConn.setDirty(true);
			}

			if (cmProxyCallbakUplink != null) {
				cmProxyCallbakUplink.cmCallStart(conn, stmt, opType);
			}
		}

		public void cmCallEnd(Connection conn, Statement stmt, JdbcOperationType opType, SQLException e) {
			if (conn == null) {
				String msg = "cmCallEnd - conn is null";
				logger.info("ODAK_CMCALL_NULL_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_CMCALL_NULL_CONN", getName(), msg, "0");
				return;
			}
			OdakConnection ocpConn = (OdakConnection) conn;
			ocpConn.decrInCallCount();
			if (cmProxyCallbakUplink != null) {
				cmProxyCallbakUplink.cmCallEnd(conn, stmt, opType, e);
			}
		}

		public SQLException cmProcessException(Connection conn, Statement stmt, JdbcOperationType opType,
				SQLException e) {
			if (conn == null) {
				String msg = "cmProcessException - conn is null";
				logger.info("ODAK_CMCALL_NULL_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_CMCALL_NULL_CONN", getName(), msg, "0");
				return e;
			}
			if (conn != null) {
				OdakConnection ocpConn = (OdakConnection) conn;
				/*
				 * Note that shouldCauseConnectionFlush(e) is always false in
				 * occjdbc driver. Existing code maintains flush level (single
				 * conn vs whole pool) but calls the same below method.
				 * 
				 */
				boolean shouldFlush = getDriverAdapter().shouldCausePoolFlush(e);
				if (shouldFlush) {
					String msg = String.format(
							"Destroying connection after the fatal exception: %s, conn: %s, pool:%s, state:%s",
							e.getMessage(), conn, getName(), StateLogger.getInstance().getState(getName()));
					CalEventHelper.writeLog("ODAK_CONN_FLUSH", getName(), msg, "0");
					logger.info("ODAK_CONN_FLUSH - " + msg);
					ocpConn.destroyConnection();
				}
			}
			if (cmProxyCallbakUplink != null) {
				return cmProxyCallbakUplink.cmProcessException(conn, stmt, opType, e);
			}

			return e;
		}

		/**
		 * Connection passed should not be wrapped.
		 */
		public void cmConnectionClose(Connection conn) throws SQLException {
			if (conn == null) {
				String msg = "cmConnectionClose - conn is null";
				logger.info("ODAK_CMCALL_NULL_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_CMCALL_NULL_CONN", getName(), msg, "0");
				return;
			}
			OdakConnection ocpConn = (OdakConnection) conn;
			ocpConn.close();
			if (cmProxyCallbakUplink != null) {
				cmProxyCallbakUplink.cmConnectionClose(conn);
			}
		}

		public void cmConnectionDestory(Connection conn) throws SQLException {
			if (conn == null) {
				String msg = "cmConnectionDestory - conn is null";
				logger.info("ODAK_CMCALL_NULL_CONN - {}", msg);
				CalEventHelper.writeLog("ODAK_CMCALL_NULL_CONN", getName(), msg, "0");
				return;
			}
			OdakConnection conn2 = (OdakConnection) conn;
			conn2.destroyConnection();
			if (cmProxyCallbakUplink != null) {
				cmProxyCallbakUplink.cmConnectionDestory(conn);
			}
		}

		public boolean cmIsUtf8Db() throws SQLException {
			if (cmProxyCallbakUplink != null) {
				return cmProxyCallbakUplink.cmIsUtf8Db();
			}
			return true;
		}

	}

	// used for test only
	@Deprecated
	public boolean exists(Connection conn) {
		if (activeConns.contains(conn) || freeConns.contains(conn)) {
			return true;
		}
		return false;
	}

	PoolConfig getConfig() {
		return config;
	}

	String getName() {
		return host;
	}

	JdbcDriverAdapter getDriverAdapter() {
		return driverAdapter;
	}

	long getBkgConnsReqs() {
		return bkgConnsReqs.get();
	}

	void incrBkgConnsReqs() {
		bkgConnsReqs.incrementAndGet();
	}

	void incrPoolResizeBkgConnsReqs() {
		poolResizeBkgConnsReqs.incrementAndGet();
	}

	long getPoolResizeBkgConnsReqs() {
		return poolResizeBkgConnsReqs.get();
	}

	long getDestroyedConns() {
		return destroyedConns.get();
	}

	void incrDestroyedConns() {
		destroyedConns.incrementAndGet();
	}

	long getClosedConns() {
		return closedConns.get();
	}

	void incrClosedConns() {
		closedConns.incrementAndGet();
	}

	long getIdleClosed() {
		return idleClosed.get();
	}

	void incrIdleClosed() {
		idleClosed.incrementAndGet();
	}

	long getActiveClosed() {
		return activeClosed.get();
	}

	void incrActiveClosed() {
		activeClosed.incrementAndGet();
	}

	long getAgedClosed() {
		return agedClosed.get();
	}

	void incrAgedClosed() {
		agedClosed.incrementAndGet();
	}

	public Statement unwrapStatement(Statement stmt) throws SQLException {
		return CmConnectionWrapper.unwrap(stmt);
	}

	/**
	 * By default, when connection is created it's in the Active state until it
	 * is added to the free list. Connection goes to IDLE state from the Active
	 * state only when it's about to be added to the free list.
	 * 
	 * Active connection will be added back to the free list and will become
	 * idle. Closed connection should not be added.
	 * 
	 * During checkout from the pool, only Idle connection can become Active.
	 * 
	 * @param conn
	 * @throws SQLException
	 */
	private void addFreeConn(OdakConnection conn) throws SQLException {
		if (conn.isClosed()) {
			String msg = String.format(
					"Connection cannot be addeded to the free list. Ignore this warning if connection state indicates closed. pool:%s, conn:%s",
					getName(), conn);
			logger.info("ODAK_CONN_ADD_POOL_FAILED - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ADD_POOL_FAILED", getName(), msg, "0");
			return;
		}
		if (!conn.tryToSetIdleFromActive()) {
			String msg = String.format(
					"Connection cannot be addeded to the free list. Cannot convert to IDLE from ACTIVE. "
							+ "Ignore this warning if connection state indicates closed. pool:%s, conn:%s",
					getName(), conn);
			logger.info("ODAK_CONN_ADD_POOL_FAILED - {}", msg);
			CalEventHelper.writeLog("ODAK_CONN_ADD_POOL_FAILED", getName(), msg, "0");
			// connection state can be closed/destroyed. We can ignore it
			// instead of adding back to active or free list.
			return;
		}
		freeConns.add(conn);
		logger.debug("Connection is successfully returned back to the pool {} {}", getName(),
				conn.getClientInfo(HeraConnection.HERA_CLIENT_CONN_ID));
	}

	private void removeIdleConn(OdakConnection conn) {
		freeConns.remove(conn);
		idleConnsCount.incrementAndGet();
		conn.destroyConnection();
	}

	private void removeOrphanConn(OdakConnection conn) {
		activeConns.remove(conn);
		orphanConnsCount.incrementAndGet();
		conn.destroyConnection();
	}

	private void addActiveConn(OdakConnection conn) {
		activeConns.add(conn);
	}

	void removeActiveConn(OdakConnection conn) {
		activeConns.remove(conn);
	}

	void removFreeConn(OdakConnection conn) {
		activeConns.remove(conn);
	}

	long getFgConnsCreated() {
		return fgConnsCreated.get();
	}

	long getBkgConnsCreated() {
		return bkgConnsCreated.get();
	}

	long getIdleConnsCount() {
		return idleConnsCount.get();
	}

	long getAgedConnsCount() {
		return agedConnsCount.get();
	}

	void incrAgedConnsCount() {
		agedConnsCount.incrementAndGet();
	}

	long getOrphanConnsCount() {
		return orphanConnsCount.get();
	}

	int getActiveConnsCount() {
		return activeConns.size();
	}

	int getFreeConnsCount() {
		return freeConns.size();
	}

	// Useful to find connections not in free or active list. Also, allows to
	// validate against the pool size.
	int getAllConnsCountByCreatedDestroyed() {
		return allConnsByCreateDestory.get();
	}

	int incrAllConnsByCreateDestory() {
		return allConnsByCreateDestory.incrementAndGet();
	}

	int decrAllConnsByCreateDestory() {
		return allConnsByCreateDestory.decrementAndGet();
	}

	int getCurrentConnsCount() {
		return activeConns.size() + freeConns.size();
	}

	/**
	 * Contract method. Returns maximum number of retries to open a new
	 * connection that DCP would do if I/O exception happen
	 */
	public int getConnectRetryCountForIoException() {
		boolean wouldRetry = driverAdapter.expectsRetryOnConnectIoException(false);
		if (wouldRetry) {
			return MAX_CONNECT_ATTEMPTS - 1;
		}
		return 0;
	}

	int getSize() {
		return size;
	}

	void setSize(int size) {
		this.size = size;
	}
	
	int[] getSpikes(){
		Integer[] data = spikes.toArray(new Integer[0]);
		return ArrayUtils.toPrimitive(data);
	}
	
	void resetSpikes(){
		spikes.clear();
	}
}
