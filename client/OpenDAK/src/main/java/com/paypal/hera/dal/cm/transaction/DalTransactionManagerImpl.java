package com.paypal.hera.dal.cm.transaction;

import com.paypal.hera.dal.cm.wrapper.CmConnectionWrapper;

/**
 * 
 * Description: 	Manager class for DalTransactions
 * 
 */
public final class DalTransactionManagerImpl
{
	
	/**
	 * Obtain the status of the transaction associated with the current thread.
	 * 
	 */
	public DalTransactionImpl getTransaction()
	{
	  return new DalTransactionImpl();	
	}

	
	public void begin() {
		// TODO Auto-generated method stub
		
	}

	
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	

	
	public int getStatus() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

	
	public void setRollbackOnly() {
		// TODO Auto-generated method stub
		
	}

	
	public void setTransactionTimeout(int seconds) {
		// TODO Auto-generated method stub
		
	}

	
	public boolean isSuspended() {
		// TODO Auto-generated method stub
		return false;
	}


	public void addConnectionForTransaction(String name, CmConnectionWrapper cmConnWrap) {
		// TODO Auto-generated method stub
		
	}


	public boolean isInTransaction() {
		// TODO Auto-generated method stub
		return false;
	}


	public boolean hasConnection(CmConnectionWrapper cmConnectionWrapper) {
		// TODO Auto-generated method stub
		return false;
	}


	public CmConnectionWrapper getConnection(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
