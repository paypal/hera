package com.paypal.hera.dal.cm.transaction;

import com.ebay.kernel.exception.BaseRuntimeException;

/**
 * 
 * Description: 	Defines runtime DaTransactionRuntimeException
 * 
 */
public class DalTransactionRuntimeException extends BaseRuntimeException
{
	public DalTransactionRuntimeException(String strMsg)
	{
		super(strMsg);
	}
	
	public DalTransactionRuntimeException(Throwable cause)
	{
		super(cause.getMessage(), cause);
	}
		
	public DalTransactionRuntimeException(String stringMessage, Object[] messageArgs)
	{
		this(stringMessage, messageArgs, null);	
	}
	
	public DalTransactionRuntimeException(String stringMessage, Throwable cause)
	{
		super(stringMessage, cause);	
	}
	
	public DalTransactionRuntimeException(String stringMessage, Object[] messageArgs, Throwable cause)
	{
		super(stringMessage, messageArgs, cause);
	}
}
