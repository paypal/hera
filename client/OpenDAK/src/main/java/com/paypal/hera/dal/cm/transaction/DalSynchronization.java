package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Defines interface, similar to javax.transaction.Synchronization.  
 * 		The transaction manager supports a synchronization mechanism that allows 
 * 		the interested party to be notified before and after the transaction completes. 
 * 		Using the registerSynchronization method, the application server registers 
 * 		a DalSynchronization object for the transaction currently associated with 
 * 		the target DalTransaction object. 
 */
public interface DalSynchronization
{
	/*========= BEGIN standard methods from javax.transaction.Synchronization: =========*/
	
	/**
	 * This method is called by the transaction manager after the transaction 
	 * 	is committed or rolled back
	 * 
	 * @throws DalTransactionException
	 */
	void afterCompletion(DalTransactionStatusEnum status);
	
	/**
	 * This method is called by the transaction manager prior to the start 
	 * 	of the transaction completion process
	 * 
	 * @throws DalTransactionException
	 */
	void beforeCompletion();
}
