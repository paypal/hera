package com.paypal.hera.dal;

import com.ebay.kernel.exception.BaseRuntimeException;
import com.ebay.kernel.message.Message;

public class DalRuntimeException extends RuntimeException {

	/**
	 * Constructor for DALRuntimeException
	 */
	public DalRuntimeException(Message message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructor for DALRuntimeException
	 */
	public DalRuntimeException(Message message) {
		super(message);
	}
	
	public DalRuntimeException(String strMsg)
	{
		super(strMsg);
	}
	
	public DalRuntimeException() {
		this((Message)null, null);
	}
	
	public DalRuntimeException(String stringMessage, Object[] messageArgs) {
		this(stringMessage, messageArgs, null);	
	}
	
	public DalRuntimeException(String stringMessage, Throwable cause) {
		super(stringMessage, cause);	
	}
	
	public DalRuntimeException(String stringMessage, Object[] messageArgs, Throwable cause) {
		super(stringMessage, messageArgs, cause);
	}
}
