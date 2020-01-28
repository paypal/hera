package com.paypal.hera.dal.cm.transaction;

/**
 * 
 * Description: 	Factory for DalTransactionsManager to get its instance
 * 
 */
public class DalTransactionManagerFactory
{
	
	/**
	 * Public method to get an instance of DalTransactionManager
	 * 
	 * @return
	 */
	public static DalTransactionManager getDalTransactionManager()
	{
		return DalTransactionManagerImpl.getInstance();
	}
}

