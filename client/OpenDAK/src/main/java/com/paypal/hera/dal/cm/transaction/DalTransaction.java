package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Defines DalTransaction interface, 
 * 					similar to javax.transaction.UserTransaction
 */
public interface DalTransaction 
{
	// defaults that we are using for new transactions if not set explicitly:
	public final int DEFAULT_TRANSACTION_TIMEOUT_SEC = 15;
	
	public final DalTransactionTypeEnum 
		DEFAULT_TRANSACTION_TYPE = DalTransactionTypeEnum.TYPE_SINGLE_DB;
	
	public final DalTransactionLevelEnum 
		DEFAULT_TRANSACTION_LEVEL = DalTransactionLevelEnum.TRANSACTION_READ_COMMITTED;
	
	public final DalTransactionStatusEnum 
		DEFAULT_TRANSACTION_STATUS = DalTransactionStatusEnum.STATUS_UNKNOWN;

	/*========= BEGIN standard methods from javax.*.UserTransaction: =========*/
	
	/**
	 * Begin a new transaction for the current thread
	 * 	NOTE: this SHOULDN'T be used directly; 
	 * 		instead, use Transaction Manager's begin()
	 * @throws DalTransactionException
	 */
	void begin() 
		throws DalTransactionException;
	
	/**
	 * Commit the transaction for the current thread;
	 * 	thread will be no longer associated with a transaction.	
	 * 
	 * @throws DalTransactionException
	 */
	void commit()
		throws DalTransactionException, DalTransactionPartialResultException;
	
	/**
	 * Obtain the status of the transaction
	 * 
	 * @throws DalTransactionException
	 */
	int getStatus();

	/**
	 * Rollback the transaction for the current thread;
	 * 	thread will be no longer associated with a transaction.
	 * 
	 * 	@throws DalTransactionException
	 */
	void rollback()
		throws DalTransactionException;
	
	/**
	 * Force transaction to perform rollback on its completion 
	 * 
	 * @throws DalTransactionException
	 */
	void setRollbackOnly()
		throws DalTransactionException;
	
	/**
	 * Change the transaction's timeout value in seconds
	 * 
	 * @throws DalTransactionException
	 */
	void setTransactionTimeout(int seconds)
		throws DalTransactionException;
	
	/**
	 * Register a synchronization object for the transaction currently associated 
	 * 	with the calling thread: the transaction invokes
	 * 	 - beforeCompletion method prior to starting the transaction commit process
	 * 	 - afterCompletion after the transaction is completed.
	 * 
	 * @throws DalTransactionException
	 */
	void registerSynchronization(DalSynchronization dalSync)
		throws DalTransactionException;

	
	/*========= END standard methods from javax.*.UserTransaction: =========*/
	
	/**
	 * Set transaction type. 
	 * 	See DalTransactionTypeEnum for the supported types
	 * 
	 * @param dalTransactionTypeEnum
	 * @throws DalTransactionException
	 */
	void setTransactionType(DalTransactionTypeEnum dalTransactionTypeEnum)
		throws DalTransactionException;
	
	/**
	 * Get transaction type. 
	 * 	See DalTransactionTypeEnum for the supported types
	 * 
	 */
	DalTransactionTypeEnum getTransactionType();

	/**
	 * Get the transaction's timeout value in seconds 
	 * 
	 */
	int getTransactionTimeout();

	/**
	 * Get the transaction's duration in milliseconds counting from begin()
	 * 
	 * @return
	 * @throws DalTransactionException
	 */
	int getTransactionDuration();
	
	/**
	 * Obtain the status of the transaction as ENUM
	 * 
	 * @throws DalTransactionException
	 */
	DalTransactionStatusEnum getStatusEnum();
	
	/**
	 * Set transaction isolation level
	 * NOTE: Oracle supports only READ_COMMITTED and SERIALIZABLE!!
	 * 
	 * @param dalTransactionLevelEnum
	 * @throws DalTransactionException
	 */
	void setTransactionLevel(DalTransactionLevelEnum dalTransactionLevelEnum)
		throws DalTransactionException;
	
	/**
	 * Get transaction isolation level
	 * 
	 * @throws DalTransactionException
	 */
	DalTransactionLevelEnum getTransactionLevel();
	
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
	 * 		ToupleProvider that creates a lookup connection to DB
	 * 
	 * @throws DalTransactionException
	 */
	DalTransaction suspend()
		throws DalTransactionException;
	
	/**
	 * Resume the transaction.
	 * 
	 * NOTE: in eBay's case, we resume returning connections back from 
	 * 		the transaction's list if it's there, or create new one and add to 
	 * 		transaction's list if it's not there.
	 * 
	 * NOTE: suspend()/resume() will be used for excluding some datasources
	 * 		or DAO methods from transaction, like in case of calling 
	 * 		ToupleProvider that creates a lookup connection to DB
	 * 
	 * @throws DalTransactionException
	 */
	void resume()
		throws DalTransactionException;
	
	/**
	 * Detect whether the transaction is suspended.
	 * 
	 * @return
	 */
	boolean isSuspended();
	
}
