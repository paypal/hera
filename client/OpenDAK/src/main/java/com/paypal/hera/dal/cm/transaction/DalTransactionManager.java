package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Defines DalTransaction interface, 
 * 					similar to javax.transaction.TransactionManager
 */
public interface DalTransactionManager
{
	/*========= BEGIN standard methods from javax.*.TransactionManager: =========*/
	
	/**
	 *  Create a new transaction and associate it with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	void begin() 
		throws DalTransactionException;
	
	/**
	 * Complete the transaction associated with the current thread, 
	 * commit the current thread's transaction
	 * 
	 * @throws DalTransactionException
	 */
	void commit()
		throws DalTransactionException, DalTransactionPartialResultException;
	
	/**
	 * Obtain the status of the transaction associated with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	DalTransaction getTransaction();

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
	void resume(DalTransaction dalTransaction)
		throws DalTransactionException;

	/**
	 * Obtain the status of the transaction associated with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	int getStatus()
		throws DalTransactionException;

	/**
	 * Rollback the transaction for the current thread;
	 * 	thread will be no longer associated with a transaction.
	 * 
	 * 	@throws DalTransactionException
	 */
	void rollback()
		throws DalTransactionException;
	
	/**
	 * Modify the transaction associated with the current thread such that 
	 *  	the only possible outcome of the transaction is to roll back 
	 *  	the transaction.
	 * 
	 * @throws DalTransactionException
	 */
	void setRollbackOnly()
		throws DalTransactionException;
	
	/**
	 * Modify the value of the timeout value (in seconds) that is associated 
	 * 	with the transactions started by the current thread with the begin method.
	 * 
	 * @throws DalTransactionException
	 */
	void setTransactionTimeout(int seconds)
		throws DalTransactionException;

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
	DalTransaction suspend()
		throws DalTransactionException;

	/**
	 * Register a synchronization object for the transaction currently associated 
	 * 	with the calling thread: the transaction invokes
	 * 	 - beforeCompletion method prior to starting the transaction commit process
	 * 	 - afterCompletion after the transaction is completed.
	 * 
	 * @throws DalTransactionException
	 */
	public void registerSynchronization(DalSynchronization dalSync)
		throws DalTransactionException;
	
	/*========= END standard methods from javax.*.TransactionManager: =========*/

	/**
	 * Get timeout value in seconds of the transaction, 
	 * 	associated with the current thread.
	 * 
	 * @throws DalTransactionException
	 */
	int getTransactionTimeout()
		throws DalTransactionException;

	/**
	 * Get the current-thread's transaction's duration in milliseconds 
	 * 		counting from begin()
	 * 
	 * @return
	 * @throws DalTransactionException
	 */
	int getTransactionDuration()
		throws DalTransactionException;
	
	/**
	 * Obtain the status of the transaction associated with 
	 * 	the current thread as ENUM
	 * 
	 * @throws DalTransactionException
	 */
	DalTransactionStatusEnum getStatusEnum()
		throws DalTransactionException;
	
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
	void setTransactionLevel(DalTransactionLevelEnum dalTransactionLevelEnum)
		throws DalTransactionException;
	
	/**
	 * Get isolation level of the transaction associated with the current thread
	 * 
	 * NOTE: Can be used ONLY before transaction becomes active, 
	 * 	that is, right after begin(), but before it connects to DB
	 * 
	 * @throws DalTransactionException
	 */
	DalTransactionLevelEnum getTransactionLevel()
		throws DalTransactionException;
	
	/**
	 * Set type of the transaction associated with the current thread
	 *
	 * NOTE: Can be used ONLY before transaction becomes active, 
	 * 	that is, right after begin(), but before it connects to DB
	 * 
	 * @param dalTransactionLevelEnum
	 * @throws DalTransactionException
	 */
	void setTransactionType(DalTransactionTypeEnum dalTransactionTypeEnum)
		throws DalTransactionException;
	
	/**
	 * Get type of the transaction associated with the current thread
	 * 
	 * @throws DalTransactionException
	 */
	DalTransactionTypeEnum getTransactionType()
		throws DalTransactionException;
	
	/**
	 * Detect whether the transaction associated with the current thread
	 * 	is suspended.
	 * 
	 * @return
	 */
	boolean isSuspended()
		throws DalTransactionException;
}
