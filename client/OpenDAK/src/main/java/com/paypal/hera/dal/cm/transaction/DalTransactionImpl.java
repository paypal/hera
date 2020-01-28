package com.paypal.hera.dal.cm.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ebay.kernel.cal.api.sync.CalEventHelper;
import com.ebay.kernel.logger.LogLevel;
import com.ebay.kernel.logger.Logger;
import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;


/**
 * 
 * Description: 	Defines DalTransaction interface, 
 * 					similar to javax.transaction.UserTransaction
 */
public class DalTransactionImpl implements DalTransaction
{
	private DalTransactionTypeEnum m_type;
	private DalTransactionLevelEnum m_level;
	private DalTransactionStatusEnum m_status;
	private boolean m_rollbackOnly;
	private boolean m_suspended;
	private int m_timeoutSec;
	private long m_timeStarted;
	private long m_timeEnded;
	// forces the transaction to be associated with trans.mgr
	private boolean m_isValid;

	// dsName-Connection pairs
	private ConcurrentMap<String, CmConnectionWrapper> m_dsNameConn;
	
	private DalSynchronization m_dalSync;

	private static Logger s_logger = null;
	private static Logger getLogger() 
 	{
		if ( s_logger == null ) {
			s_logger = Logger.getInstance(DalTransactionImpl.class);
		}
		return s_logger;
	}
	
	/**
	 * Note: the name must be either NULL when trans is for ALL datasources, 
	 * 	or physical datasource's name for which the transaction is used
	 * 
	 * @param name
	 */
	protected DalTransactionImpl()
	{
		m_type = DEFAULT_TRANSACTION_TYPE;
		m_level = DEFAULT_TRANSACTION_LEVEL;
		m_status = DEFAULT_TRANSACTION_STATUS;
		m_rollbackOnly = false;
		m_suspended = false;
		m_isValid = true;
		m_timeoutSec = DEFAULT_TRANSACTION_TIMEOUT_SEC;
		m_timeStarted = 0;
		m_timeEnded = 0;
		m_dsNameConn = new ConcurrentHashMap<>(3);
		m_dalSync = null;
	}

	/******** "standard" methods from javax.transaction.UserTransaction: ********/
	
	/**
	 * Begin a new transaction for the current thread
	 * 
	 * @throws DalTransactionException
	 */
	public void begin() 
		throws DalTransactionException
	{
		throw new DalTransactionException("Transactions cannot be re-used! "
			+ "Please, call DalTransactionManager.begin() to create a new transaction!");
	}
	
	// to be used by Trans Manager only
	void beginInternal()
		throws DalTransactionException
	{	
		m_status = DalTransactionStatusEnum.STATUS_NO_TRANSACTION;
		m_timeStarted = System.currentTimeMillis();
		
		// add yourself to the list
		DalTransactionManagerImpl.getInstance().addDalTrans(this);
		
		DalTransactionCalHelper.logCALTransaction(this, "begin()", "0", null);
	}
	
	/**
	 * Commit the transaction for the current thread;
	 * 	thread will be no longer associated with a transaction.	
	 * 
	 * @throws DalTransactionException
	 */
	public void commit()
		throws DalTransactionException, DalTransactionPartialResultException
	{
		checkExpired();
		checkValid();
		checkSuspended();
		
		// don't allow commits on rollback-only transactions:
		if (m_rollbackOnly) {
			throw new DalTransactionException("Cannot COMMIT on RollBackOnly transactions!");
		}
		
		// callback:
		beforeCompletion();
		
		DalTransactionCalHelper.logCALTransaction(this, "commit()", "0", null);
		m_status = DalTransactionStatusEnum.STATUS_COMMITTING;
		List<String> successHosts = new ArrayList<String>();
		List<String> failedHosts = new ArrayList<String>();
		Throwable lastError = null;
		for (Iterator<String> it = m_dsNameConn.keySet().iterator(); it.hasNext(); ) {
			String dsName = it.next();
			CmConnectionWrapper conn = m_dsNameConn.get(dsName);
			try {
				if (lastError != null && m_type == DalTransactionTypeEnum.TYPE_MULTI_DB_ON_FAILURE_ROLLBACK) {
					rollback();
					failedHosts.add(dsName);
				} else {
					conn.commit();
					successHosts.add(dsName);
				}
			} catch (SQLException sqlex) {
				lastError = sqlex;
				failedHosts.add(dsName);
				getLogger().log(LogLevel.ERROR, sqlex);
			}
		}
		m_status = DalTransactionStatusEnum.STATUS_COMMITTED;
		
		m_timeEnded = System.currentTimeMillis();
		
		// callback:
		afterCompletion(m_status);

		if(lastError == null) {
			// remove the transaction to prevent its re-using
			DalTransactionCalHelper.endCALTransaction(this);
			DalTransactionManagerImpl.getInstance().removeDalTrans(this);

			// release all connections back to DCP
			closeConnections();

			m_isValid = false;
		} else {
			String errMsg =
				"DalTransaction: the COMMIT was just partially successful;" +
				" SUCCEDED hosts: " + successHosts  +
				" FAILED hosts: " + failedHosts  +
				"; see previous logged errors!"  +
				" LAST error: " + lastError.getMessage();
			throw new DalTransactionPartialResultException(errMsg, successHosts, failedHosts);
		}
	}
	
	/**
	 * Rollback the transaction for the current thread;
	 * 	thread will be no longer associated with a transaction.
	 * 
	 * 	@throws DalTransactionException
	 */
	public void rollback()
		throws DalTransactionException
	{
		//checkValid();
		//checkExpired();
		//checkSuspended();

		// callback:
		beforeCompletion();

		DalTransactionCalHelper.logCALTransaction(this, "rollback()", "0", null);
		
		m_status = DalTransactionStatusEnum.STATUS_ROLLING_BACK;
		for (Iterator<String> it = m_dsNameConn.keySet().iterator(); it.hasNext(); ) {
			String dsName = it.next();
			CmConnectionWrapper conn = m_dsNameConn.get(dsName);
			try {
				conn.rollback();
			} catch (Throwable th) {
				// just log it here, and continue
				getLogger().log(LogLevel.ERROR, th);
				String msg = "Rollback failed. Connection will be force-destroyed.";
				CalEventHelper.writeLog("DAL_ROLLBACK_FAILED", dsName, msg, "0");		
				conn.forceDestroy();
			}
		}
		
		m_status = DalTransactionStatusEnum.STATUS_ROLLEDBACK;
		
		m_timeEnded = System.currentTimeMillis();
		
		// callback:
		afterCompletion(m_status);
		
		// Call this before removing DalTrans: otherwise we'll see ChildNotCompleted in CAL
		// (NOTE: if this is called from a different thread, then we'll still ChildNotCompleted in CAL)
		DalTransactionCalHelper.endCALTransaction(this);
		
		// NB: don't allow reusing transactions!
		DalTransactionManagerImpl.getInstance().removeDalTrans(this);
		DalTransactionManagerImpl.getInstance().cleanKilledDalTrans(this);
		
		// release all connections back to DCP
		closeConnections();
		m_isValid = false;
	}
	
	/**
	 * Force transaction to perform rollback on its completion 
	 * 
	 * @throws DalTransactionException
	 */
	public void setRollbackOnly()
		throws DalTransactionException
	{
		m_rollbackOnly = true;
	}

	/**
	 * Obtain the status of the transaction. 
	 * 
	 * @throws DalTransactionException
	 */
	public int getStatus()
	{
		return m_status.ordinal();
	}
	
	/**
	 * Change the transaction's timeout value in seconds
	 * 
	 * @throws DalTransactionException
	 */
	public void setTransactionTimeout(int seconds)
		throws DalTransactionException
	{
		m_timeoutSec = seconds;
	}
	
	/**
	 * Register a synchronization object for the transaction currently associated 
	 * 	with the calling thread: the transaction invokes
	 * 	 - beforeCompletion method prior to starting the transaction commit process
	 * 	 - afterCompletion after the transaction is completed.
	 * 
	 * @throws DalTransactionException
	 */
	public void registerSynchronization(DalSynchronization dalSync)
		throws DalTransactionException
	{
		// check that we are in right state
		if (!isInactive()) {
		 	throw new DalTransactionException(
		 		"The transaction's registerSynchronization() method can be " +
		 		"called only before its usage!");
		}
		m_dalSync = dalSync;
	}

	
	/*========= END standard methods from javax.*.UserTransaction: =========*/
	
	/**
	 * Set transaction type. 
	 * 	See DalTransactionTypeEnum for the supported types
	 * 
	 * @param dalTransactionTypeEnum
	 * @throws DalTransactionException
	 */
	public void setTransactionType(DalTransactionTypeEnum dalTransactionTypeEnum)
		throws DalTransactionException
	{
		// check that we are in right state
		if (!isInactive()) {
		 	throw new DalTransactionException(
		 		"The transaction's TYPE can only be changed before its usage!");
		}
		
		m_type = dalTransactionTypeEnum;
	}
	
	/**
	 * Get transaction type. 
	 * 	See DalTransactionTypeEnum for the supported types
	 * 
	 */
	public DalTransactionTypeEnum getTransactionType()
	{
		return m_type;
	}
	
		
	/**
	 * Get the transaction's timeout value in seconds 
	 * 
	 */
	public int getTransactionTimeout()
	{
		return m_timeoutSec;
	}

	/**
	 * Get the transaction's duration in milliseconds counting from begin()
	 * 
	 * @return
	 * @throws DalTransactionException
	 */
	public int getTransactionDuration()
	{
		return (m_timeStarted == 0 ? 0 :
				m_timeEnded > 0 ? (int)(m_timeEnded - m_timeStarted) :
				 (int)(System.currentTimeMillis() - m_timeStarted)
			);
	}
	
	/**
	 * Obtain the status of the transaction as ENUM
	 * 
	 */
	public DalTransactionStatusEnum getStatusEnum()
	{
		return m_status;
	}
	
	// Package usage only
	void setStatusEnum(DalTransactionStatusEnum status)
	{
		m_status = status;
	}
	
	/**
	 * Set transaction isolation level
	 * 
	 * @param dalTransactionLevelEnum
	 * @throws DalTransactionException
	 */
	public void setTransactionLevel(DalTransactionLevelEnum dalTransactionLevelEnum)
		throws DalTransactionException
	{
		// check that we are in right state
		if (!isInactive()) {
		 	throw new DalTransactionException(
		 		"The transaction's LEVEL can only be changed before its usage!");
		}
		
		m_level = dalTransactionLevelEnum;
	}
	
	/**
	 * Get transaction isolation level
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransactionLevelEnum getTransactionLevel()
	{
		return m_level;
	}
	
	/**
	 * Check whether the transaction is inactive, usually right after begin,
	 * 	but before first operation is performed.
	 * 
	 * 	we are re-using same object instead of creating new ones each time
	 * 
	 * @return
	 */
	boolean isInactive()
	{
		return (m_status == DalTransactionStatusEnum.STATUS_NO_TRANSACTION);
	}
	
	boolean isValid()
	{
		return m_isValid 
			&& DalTransactionManagerImpl.getInstance().containsDalTrans(this);
	}
	
	// prevent transactions from being re-used without TransactionManager:
	private void checkValid()
		throws DalTransactionException
	{
		if (isValid()) {
			return;
		}
		
		DalTransactionException dtre = new 
			DalTransactionException(
			"DalTransaction is INVALID and cannot be re-used; " +
			"please, call transManager.begin() to create a new one!");
		throw dtre;
	}
	
	long getTimeStarted()
	{
		return m_timeStarted;
	}
	
	long getTimeEnded()
	{
		return m_timeEnded;
	}
	
	/**
	 * Check whether the transaction is unused: 
	 * 	we are re-using same object instead of creating new ones each time
	 * 
	 * @return
	 */
	boolean isUnused()
	{
		return (m_status == DEFAULT_TRANSACTION_STATUS);
	}
	
	/** 
	 * check whether the transaction, associated with the current thread, 
	 * 	 has expired
	 * 
	 * @return
	 */
	boolean isExpired()
	{
		return (m_timeStarted > 0 && 
			System.currentTimeMillis() - m_timeStarted > m_timeoutSec * 1000L);
	}
		
	private void checkExpired()
		throws DalTransactionException
	{
		if (!isExpired()) {
			return;
		}
		
		DalTransactionCalHelper.logCALTransaction(this, "EXPIRED", "Expired!", null);
		m_isValid = false;
		DalTransactionException dte = new DalTransactionException(
			"DalTransactionException has been EXPIRED; " +
			"all uncommited DB changes were attempted to be rolled-back!");
		// reset(); -- don't reset to keep stats
		throw dte;
	}
	
	private void checkSuspended()
	{
		if (!isSuspended()) {
			return;
		}
		
		DalTransactionRuntimeException dtre = new 
			DalTransactionRuntimeException(
			"DalTransaction is SUSPENDED and cannot be used; " +
			"please, call resume() before using it!");
		throw dtre;
	}
	
	private void beforeCompletion() 
		throws DalTransactionException
	{
		if (m_dalSync == null) {
			return;
		}
		
		try {
			m_dalSync.beforeCompletion();
		}
		catch (Throwable th) {
			throw new DalTransactionException(
				"Failure in beforeCompletion()", th);
		}
	}
	
	private void afterCompletion(DalTransactionStatusEnum status) 
		throws DalTransactionException
	{
		if (m_dalSync == null) {
			return;
		}
		
		try {
			m_dalSync.afterCompletion(status);
		}
		catch (Throwable th) {
			throw new DalTransactionException(
				"Failure in afterCompletion()", th);
		}
	}
	
	public boolean isSuspended()
	{
		return m_suspended;
	}
	
	// NOTE: to be used only from KernalDAL
	Connection getConnection(String dsName)
	{
		return  m_dsNameConn.get(dsName);
	}
	
	// NOTE: to be used only from KernalDAL
	void addConnection(String dsName, CmConnectionWrapper conn)
	{
		if (m_type == DalTransactionTypeEnum.TYPE_SINGLE_DB && 
			!m_dsNameConn.isEmpty()) {
			DalTransactionRuntimeException drte = 
				new DalTransactionRuntimeException(
				"DalTransaction cannot involve multipe databases because " +
				"its type was set as " +
					DalTransactionTypeEnum.TYPE_SINGLE_DB.name() +
				"There is alredy 1 DB participating in transaction: " +
				 m_dsNameConn.keySet() +
				"; however, a second DB connection was requested for dsName: "
				 + dsName);
			throw drte;
		}
		m_dsNameConn.put(dsName, conn);
		
		// change status to active, in order to prohibit type/level changes
		m_status = DalTransactionStatusEnum.STATUS_ACTIVE;
	}
	
	
	
	boolean hasConnection(Connection conn)
	{
		if (isUnused()) {
			return false;
		}
		return m_dsNameConn.containsValue(conn);
	}
	
	/**
	 * Termiate the transaction because it expired
	 * 	Rollback all its connecitons, if necessary, 
	 *   and release them for being used by DCP
	 * 
	 * @throws DalTransactionException
	 */
	void terminate() {
		DalTransactionCalHelper.logCALTransaction(this, "terminate()", "Terminated!", null);
		// NB: rollback could be already performed by DCP
		m_status = DalTransactionStatusEnum.STATUS_ROLLING_BACK;
		for (Iterator<String> it = m_dsNameConn.keySet().iterator(); it.hasNext();) {
			String dsName = it.next();
			CmConnectionWrapper conn = m_dsNameConn.get(dsName);
			try {
				conn.rollback();
			} catch (Throwable th) {
				// don't give error to user. conn will be destroyed anyway.
				getLogger().log(LogLevel.ERROR, th);
			}
			String msg = "Transaction terminated. Connection will be force-destroyed.";
			CalEventHelper.writeLog("DAL_TX_TERMINATED", dsName, msg, "0");			
			conn.forceDestroy();
		}
		m_dsNameConn.clear();
		m_timeEnded = System.currentTimeMillis();

		m_status = DalTransactionStatusEnum.STATUS_MARKED_ROLLBACK;
		
		// prevent this transaction from being [re]used
		m_isValid = false;
		DalTransactionCalHelper.endCALTransaction(this);
	
	}
	
	private void closeConnections()
	{
		// NB: conn.wrapper will not release the conn when trans is active
		m_status = DalTransactionStatusEnum.STATUS_UNKNOWN;
	
		for (Iterator<String> it = m_dsNameConn.keySet().iterator(); it.hasNext(); ) {
			String dsName = it.next();
			CmConnectionWrapper conn = m_dsNameConn.get(dsName);
			if (conn == null || conn.isForcedDestroy()) {
				// don't return destroyed conn to pool
				continue;
			}
			try {
				if (conn.isClosed()) {
					// Should never happen:
					throw new DalTransactionRuntimeException(
						"Internal error: closed connection for datasource: " + dsName);
				}
				conn.close();
			} catch (Exception ex) {
				// just log it
				getLogger().log(LogLevel.ERROR, 
					"Error closing conneciton for dsName="+dsName, ex);
			}
		}
		
		m_dsNameConn.clear();
	}
	
	/**
	 * Suspend the transaction
	 * 
	 * NOTE: in eBay's case, when a new connection is requested, we'll not 
	 * 		try to get it from the transaction, even if it's there on the list;
	 * 		hence, the getConneciton() calls will return connections
	 * 		in the AUTOCOMMIT mode from DCP.
	 * 
	 * NOTE: suspend()/resume() will be used for excluding some datasources
	 * 		or DAO methods from transaction, like in case of calling 
	 * 		ToupleProvider that creates a lookup connection to DB, and 
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransaction suspend()
		throws DalTransactionException
	{
		checkValid();
		checkExpired();
		
		m_suspended = true;
		
		return this;
	}

	/**
	 * Resume the transaction.
	 * 
	 * NOTE: in eBay's case, we resume returning connections back from 
	 * 		the transaction's list if it's there, or create new one and add to 
	 * 		transaction's list if it's not there.
	 * 
	 * NOTE: suspend()/resume() will be used for excluding some datasources
	 * 		or DAO methods from transaction, like in case of calling 
	 * 		ToupleProvider that creates a lookup connection to DB, and 
	 * 
	 * @throws DalTransactionException
	 */
	public void resume()
		throws DalTransactionException
	{
		checkValid();
		checkExpired();
		
		m_suspended = false;
	}
	
}
