package com.paypal.hera.dal.cm.transaction;

import java.util.List;

import com.ebay.kernel.exception.BaseException;

/**
 * 
 * Description: 	DalTransactionPartialResultException is thrown when
 * 				we are attempting commit() or rollback() on multiple datasources, 
 * 				and some of them succeed while others fail
 * 
 */
public class DalTransactionPartialResultException extends BaseException
{
	private List<String> m_successHosts;
	private List<String> m_failedHosts;
	
	public DalTransactionPartialResultException(String msg, List<String> successHosts, List<String> failedHosts)
	{
		super(msg);
		m_successHosts = successHosts;
		m_failedHosts = failedHosts;
	}
	
	public DalTransactionPartialResultException(Throwable t)
	{
		super(t.getMessage(), t);
	}

	public DalTransactionPartialResultException(String msg, Throwable th)
	{
		super(msg, th);
	}
	
	/**
	 * Get the list of DB hosts that succeeded on their connection's commit()
	 * @return
	 */
	public List<String> getSuccessHosts()
	{
		return m_successHosts;
	}
	
	/**
	 * Get the list of DB hosts that failed on their connection's commit()
	 * @return
	 */
	public List<String> getFailedHosts()
	{
		return m_failedHosts;
	}

}
