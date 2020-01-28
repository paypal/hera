package com.paypal.hera.dal.cm.transaction;

import com.ebay.kernel.exception.BaseException;

/**
 * 
 * Description: 	Defines generic DalTransactionException
 * 
 */
public class DalTransactionException extends BaseException
{

	public DalTransactionException(String msg)
	{
		super(msg);
	}
	
	public DalTransactionException(Throwable t)
	{
		super(t.getMessage(), t);
	}

	public DalTransactionException(String msg, Throwable th)
	{
		super(msg, th);
	}

}
