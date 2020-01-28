package com.paypal.hera.dal.cm.transaction;

import java.sql.Connection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ebay.kernel.logger.LogLevel;
import com.ebay.kernel.logger.Logger;
import com.paypal.hera.dal.DalStaleConnectionException;
import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;
import com.paypal.integ.odak.OdakConfigManager;

/**
 * 
 * Description: 	Manager class for DalTransactions
 * 
 */
public final class DalTransactionManagerImpl implements DalTransactionManager
{
	private static final String DAL_TRANS_MONITOR_THREAD_NAME = "DalTransactionMonitorThread"; 
		
	private final static DalTransactionManagerImpl INSTANCE = new DalTransactionManagerImpl();
	
	private Thread m_dalTransMonitorThread;
	private int m_dalTransMonitorThreadId = -1;
	
	// all current transactions: threadID -> transaction map
	private ConcurrentMap<Long, DalTransactionImpl> m_allTrans;
	
	/* IMPORTANT NOTE about terminated transactions: 
	 * 	 we'll keep throwing exceptions until the transaction is rolled-back
	 * 	 explicitly; otherwise the user might NOT notice that his transaction 
	 * 	 was killed (because it took to long to complete), so he will continue 
	 * 	 doing DAO calls in autocommit, thinking that they are in-transaaction.
	 */
	private ConcurrentMap<Long, DalTransactionImpl> m_killedTrans;
	
	private static class LoggerHolder { 
        public static final Logger s_logger = Logger.getInstance(DalTransactionManagerImpl.class);
	}
	
	private static Logger getLogger() {
		return LoggerHolder.s_logger;
	}
	
    private DalTransactionManagerImpl()
    {
    	m_allTrans = new ConcurrentHashMap<Long, DalTransactionImpl>();
    	m_killedTrans = new ConcurrentHashMap<Long, DalTransactionImpl>();
    	
    }

    /** 
     * Get instance of the class, which is a singleton: make it package-protected
     * 	to allow calls only from Factory and prevent direct calls
     * 
     * @return
     */
    static DalTransactionManagerImpl getInstance() {
		return INSTANCE;
	}
		
	/*========= BEGIN standard methods from javax.*.TransactionManager: =========*/
	
	/**
	 *  Create a new transaction and associate it with the current thread.
	 *  NOTE: for now, we don't support multiple exceptions per thread, with 
	 *  	one active at a time, as assumed by javax.*.TransactionManager
	 *  	instead, we allow only 1 transaction per thread, and throw
	 *  	an exception if another one already exists
	 * 
	 * @throws DalTransactionException
	 */
	public void begin() 
		throws DalTransactionException
	{
		checkKilled();
		DalTransactionImpl dalTrans = getTransaction();
		if (dalTrans != null) {
			throw new DalTransactionException(
				"There is already associated transaction for this thread!" + 
				" Its status: " + dalTrans.getStatusEnum().name());
		}
		
		// start monitoring transactions if not started yet:
		startDalTransMonitorThread();
		
		dalTrans = new DalTransactionImpl();
		dalTrans.beginInternal();
	}
	
	/**
	 * Complete the transaction associated with the current thread, 
	 * commit the current thread's transaction
	 * 
	 * @throws DalTransactionException
	 */
	public void commit()
		throws DalTransactionException, DalTransactionPartialResultException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.commit();
	}
	
	void addDalTrans(DalTransactionImpl dalTrans)
	{
		m_allTrans.put(getThreadId(), dalTrans);
	}
	
	void removeDalTrans(DalTransactionImpl dalTrans)
	{
		if (!containsDalTrans(dalTrans)) {
			return;
		}
		m_allTrans.remove(getThreadId());
	}
	
	boolean containsDalTrans(DalTransactionImpl dalTrans)
	{
		return m_allTrans.containsValue(dalTrans);
	}
	
	/**
	 * Obtain the status of the transaction associated with the current thread.
	 * 
	 */
	public DalTransactionImpl getTransaction()
	{
		return m_allTrans.get(getThreadId());		
	}

	/**
	 * Resume the transaction context association of the calling thread with 
	 * 	the transaction represented by the supplied Transaction object.
	 * 
	 * NOTE: in eBay's case, when a new connection is requested, we'll not 
	 * 		try to get it from the transaction, even if it's there on the list;
	 * 		hence, the getConneciton() calls will return connections
	 * 		in the AUTOCOMMIT mode from DCP.
	 * 
	 * NOTE: suspend()/resume() will be used for excluding some datasources
	 * 		or DAO methods from transaction, like in case of calling 
	 * 		ToupleProvider that creates a lookup connection to DB
	 * 
	 * @throws DalTransactionException
	 */
	public void resume(DalTransaction dalTransaction)
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		
		if (!dalTrans.equals(dalTransaction)) {
			throw new DalTransactionException(
				"There provided transaction doesn't match the one, "
				+ " which is associated with the current thread!");
		}

		dalTrans.resume();
	}

	/**
	 * Obtain the status of the transaction associated with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	public int getStatus()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getStatus();
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
		DalTransactionImpl dalTrans = getTransaction();
		if (dalTrans == null) {
			// try to get it from killed ones:
			dalTrans = m_killedTrans.get(getThreadId());
		}
		if (dalTrans != null) {
			dalTrans.rollback();
		}
	}
	
	/**
	 * Modify the transaction associated with the current thread such that 
	 *  	the only possible outcome of the transaction is to roll back 
	 *  	the transaction.
	 * 
	 * @throws DalTransactionException
	 */
	public void setRollbackOnly()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.setRollbackOnly();
	}
	/**
	 * Modify the value of the timeout value (in seconds) that is associated 
	 * 	with the transactions started by the current thread with the begin method.
	 * 
	 * @throws DalTransactionException
	 */
	public void setTransactionTimeout(int seconds)
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.setTransactionTimeout(seconds);
	}
	
	/**
	 * Suspend the transaction currently associated with the calling thread 
	 * 	and return a Transaction object that represents the transaction context
	 * 	 being suspended.
	 * 
	 * NOTE: in eBay's case, when a new connection is requested, we'll not 
	 * 		try to get it from the transaction, even if it's there on the list;
	 * 		hence, the getConneciton() calls will return connections
	 * 		in the AUTOCOMMIT mode from DCP.
	 * 
	 * NOTE: suspend()/resume() will be used for excluding some datasources
	 * 		or DAO methods from transaction, like in case of calling 
	 * 		ToupleProvider that creates a lookup connection to DB
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransaction suspend()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.suspend();
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
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.registerSynchronization(dalSync);
	}
	
	/*========= END standard methods from javax.*.TransactionManager: =========*/

	/**
	 * Get timeout value in seconds of the transaction, 
	 * 	associated with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	public int getTransactionTimeout()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getTransactionTimeout();
	}
	
	/**
	 * Get the current-thread's transaction's duration in milliseconds 
	 * 		counting from begin()
	 * 
	 * @return
	 * @throws DalTransactionException
	 */
	public int getTransactionDuration()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getTransactionDuration();
	}
	
	/**
	 * Obtain the status of the transaction associated with 
	 * 	the current thread as ENUM
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransactionStatusEnum getStatusEnum()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getStatusEnum();
	}
	
	/**
	 * Set isolation level of the transaction associated with the current thread
	 * 
	 * NOTE: Can be used ONLY before transaction becomes active, 
	 * 	that is, right after begin(), but before it connects to DB
	 * 
	 * NOTE: Oracle supports only READ_COMMITTED and SERIALIZABLE!!
	 * 
	 * @param dalTransactionLevelEnum
	 * @throws DalTransactionException
	 */
	public void setTransactionLevel(DalTransactionLevelEnum dalTransactionLevelEnum)
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.setTransactionLevel(dalTransactionLevelEnum);
	}
	/**
	 * Get isolation level of the transaction associated with the current thread
	 * 
	 * NOTE: Can be used ONLY before transaction becomes active, 
	 * 	that is, right after begin(), but before it connects to DB
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransactionLevelEnum getTransactionLevel()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getTransactionLevel();
	}
	
	/**
	 * Set type of the transaction associated with the current thread
	 *
	 * NOTE: Can be used ONLY before transaction becomes active, 
	 * 	that is, right after begin(), but before it connects to DB
	 * 
	 * @param dalTransactionLevelEnum
	 * @throws DalTransactionException
	 */
	public void setTransactionType(DalTransactionTypeEnum dalTransactionTypeEnum)
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		dalTrans.setTransactionType(dalTransactionTypeEnum);
	}
	
	/**
	 * Get type of the transaction associated with the current thread
	 * 
	 * @throws DalTransactionException
	 */
	public DalTransactionTypeEnum getTransactionType()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.getTransactionType();
	}
	
	/**
	 * Detect whether the transaction associated with the current thread
	 * 	is suspended.
	 * 
	 * @return
	 */
	public boolean isSuspended()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = tryGetTransaction();
		return dalTrans.isSuspended();
	}

		
	private DalTransactionImpl tryGetTransaction()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = getTransaction();
		if (dalTrans == null) {
			throw new DalTransactionException(
				"There is currently no transaction for the current thread!");
		}
		
		return dalTrans;
	}

	/** 
	 * Add a new connection to the transaction, associated with current thread
	 * 
	 * NOTE: Should be called only by KernelDAL only!
	 *  
	 * @param dsName
	 * @param conn
	 */
	public void addConnectionForTransaction(String dsName, CmConnectionWrapper conn)
	{
		try {
			DalTransactionImpl dalTrans = tryGetTransaction();
			dalTrans.addConnection(dsName, conn);
		} catch (Throwable th) {
			getLogger().log(LogLevel.ERROR, th);
			throw new DalTransactionRuntimeException(th);
		}
	}
		
	// Note: to be used only from KernelDAL
	public Connection getConnection(String dsName)
		throws DalStaleConnectionException
	{
		try {
			checkKilled();
		} catch (DalTransactionException dte) {
			throw new DalStaleConnectionException(dte.getMessage(), dte);
		}
		
		DalTransactionImpl dalTrans = getTransaction();
		if (dalTrans == null) {
			return null;
		}
		return dalTrans.getConnection(dsName);
	}
	
	
	private void checkKilled()
		throws DalTransactionException
	{
		DalTransactionImpl dalTrans = m_killedTrans.get(getThreadId());
		if (dalTrans == null) {
			return;
		}
				
		// clean it, if it was explicitly rolled-back:
		if (dalTrans.getStatusEnum() == 
				DalTransactionStatusEnum.STATUS_ROLLEDBACK) {
			m_killedTrans.remove(getThreadId());
			return;
		}
		
		// prepare error message
		String msg = "DAL transaction has been timed-out and was terminated. "
			+ "The associated database connection has been closed, but the caller "
			+ "still needs to call transMgr.rollback() to acknowledge it."
			+ " Trans started: " + (new Date(dalTrans.getTimeStarted()))
			+ "; Trans killed: " + (new Date(dalTrans.getTimeEnded()))
			+ "; Trans duration (ms): " + dalTrans.getTransactionDuration();
		
		// attempt to rollback it:
		try {
			dalTrans.rollback();
		} catch (Exception ex) {
			getLogger().log(LogLevel.ERROR, "Failed to rollback() transaction", ex);
		}
		
		throw new DalTransactionException(msg);
	}
	
	/**
	 * Return true, if the connection is in the CURRENT THREAD's transaction
	 * Should be called only by KernelDAL only!
	 * 
	 * NOTE: Should be called only by KernelDAL only!
	 * 
	 * NOTE: it's possible to "have connection", but not be in transaction
	 * 		because it's been suspended
	 * 
	 * @param conn
	 * @return
	 */
	public boolean hasConnection(Connection conn)
	{
		DalTransactionImpl dalTrans = getTransaction();
		if (dalTrans == null) {
			return false;
		}
		return dalTrans.hasConnection(conn);
	}
	
	
	/**
	 * Return true, if the CURRENT THREAD is in transaction; 
	 * 	return false otherwise, or if the transaction is suspended
	 * 
	 * @return
	 */
	public boolean isInTransaction()
	{
		// force "in-transaction" if it was killed, to force it's clening
		if (isTransactionKilled()) {
			return true;
		}

		DalTransactionImpl dalTrans = getTransaction();


		// wondering why are we checking if killed again? - truly this code should be re-factored as we are migrating to ODAK we are creating this temp workaround
		// Note we move the transaction from m_allTrans to m_killedTrans in sync block.
		// There could be a GC pause between isTransactionKilled and getTransaction (above code)
		// Meaning checked killed is empty and At that same time txn timeout thread could have deleted it from getTransaction resulting in dalTrans to null
		// checking is killed again will avoid giving a new connection to a killed transaction

		// To simulate this issue add a sleep between getTransaction and isTransactionKilled above this comment above getTransaction
		// make sure by the time this thread is sleeping transaction timeout thread runs and cleans the record
		// by doing so you can see transaction is turning out into autocommit.
		if (dalTrans == null && isTransactionKilled())
			return true;

		if (dalTrans == null || dalTrans.isUnused() || dalTrans.isSuspended()) {
			return false;
		}

		
		return true;
	}
	
	private boolean isTransactionKilled()
	{
		return m_killedTrans.containsKey(getThreadId());
	}
	
	private Long getThreadId()
	{
		return Long.valueOf(Thread.currentThread().getId());
	}
	
	private Map<Long, DalTransactionImpl> getAllTransactions()
	{
		return m_allTrans;
	}
	
	DalTransaction getDalTransaction(Long threadId)
	{
		return m_allTrans.get(threadId);
	}
	
	// called from a different thread
	void terminate(Long threadId, DalTransactionImpl dalTrans)
	{
		// remove it from the current map, and move to killed
		synchronized(m_allTrans) {
			m_killedTrans.put(threadId, dalTrans);
			m_allTrans.remove(threadId);
		}

		dalTrans.terminate();
	}
	
	void cleanKilledDalTrans(DalTransactionImpl dalTrans)
	{
		if (!m_killedTrans.containsValue(dalTrans)) {
			return;
		}
		m_killedTrans.remove(getThreadId());
	}
	
	// launch transaction monintoring thread
	private synchronized void startDalTransMonitorThread()
	{
		// don't start it if it's already there
		if (isDalTransMonitorThreadAlive()) {
			return;
		}
		
		m_dalTransMonitorThreadId++;
		String threadName = DAL_TRANS_MONITOR_THREAD_NAME + m_dalTransMonitorThreadId;
		getLogger().log(LogLevel.WARN, "Starting thread: " + threadName);
		
		m_dalTransMonitorThread = new Thread(new DalTransMonitorThread(), threadName); //ThreadPCR# PLATS10758662
		m_dalTransMonitorThread.setDaemon(true);
		m_dalTransMonitorThread.start();
	}
	
	private boolean isDalTransMonitorThreadAlive()
	{
		return m_dalTransMonitorThread != null && m_dalTransMonitorThread.isAlive();
	}
		
	/**
	 * Monitor all transactions, and terminate the ones that has expired -- PLATS10758662
	 * 
	 * This is an "on demand" thread: we start it when at least 1 transaction
	 * 	gets created; and then we exit if we don't see any transactions 
	 * 	for 30 minutes; it gets re-started when transactions are used, again.
	 * 
	 */
	private static class DalTransMonitorThread extends Thread
	{
		private static final long MAX_IDLE_TIME_MS = 24 * 60 * 60 * 1000;
		private static final int SLEEP_TIME_MS = 5000;
			
		private boolean m_shouldStop = false;
		private long m_activeTransLastTime;
		
		boolean reachedMaxIdleTime()
		{
			return (m_activeTransLastTime > 0 && 
					System.currentTimeMillis() - m_activeTransLastTime > MAX_IDLE_TIME_MS);
		}

		protected DalTransMonitorThread()
		{
			//ThreadPCR# PLATS10758662
		}

		public void safeStop() {
			m_shouldStop = true;
		}

		private boolean shouldRun()
		{
			return (!m_shouldStop && !reachedMaxIdleTime());
		}
		
		public void run()
		{
			while (shouldRun()) {
				try {
					try {
						sleep(SLEEP_TIME_MS);
					} catch (InterruptedException e) {
						; // ignore
					}
					checkDalTransactions();
				} catch (Throwable th) {
					getLogger().log(LogLevel.ERROR, th);
				}
			}
		}
		
		// check all existing transactions, and terminate the expired ones
		private void checkDalTransactions()
		{
			Map<Long, DalTransactionImpl> allTrans = new 
				HashMap<Long, DalTransactionImpl>(INSTANCE.getAllTransactions());

			if (allTrans.isEmpty()) {
				return;
			}
			
			// have active transactions
			m_activeTransLastTime = System.currentTimeMillis();
			
			for (Iterator<Long> it = allTrans.keySet().iterator(); it.hasNext(); ) {
				long threadId = it.next();
				DalTransactionImpl dalTrans = allTrans.get(threadId);
				if (dalTrans.isExpired()) {
					INSTANCE.terminate(threadId, dalTrans);
				}
			}
		}
	}
}
