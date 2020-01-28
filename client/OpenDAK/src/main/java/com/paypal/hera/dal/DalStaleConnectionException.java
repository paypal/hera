package com.paypal.hera.dal;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

import com.ebay.kernel.exception.CommonException;
import com.ebay.kernel.exception.ErrorData;
import com.ebay.kernel.exception.GenericException;

/**
 * This class can be thrown by drivers that internally
 * map their exceptions to stale.  The search driver
 * type internally maps the proper exceptions to this type
 * of exception, so no special exception decoder is needed
 * when this exception is used directly.
 */
public class DalStaleConnectionException extends SQLException 
	implements GenericException, CommonException.SuperPrintStackTrace
{
	private CommonException m_commonException;
	
	public DalStaleConnectionException(String msg)
	{
		super(msg);
	}
	
	public DalStaleConnectionException(String msg, Throwable th)
	{
		super(msg);
		m_commonException = new CommonException(this, msg, th);
	}

	public DalStaleConnectionException(Throwable th) 
	{
		this(th.getMessage(), th);
	}
	
	public Throwable getCause() 
	{
		return m_commonException.getCause();
	}
	/* (non-Javadoc)
	 * @see com.ebay.kernel.exception.GenericException#getErrorData()
	 */
	public ErrorData getErrorData()
	{
		return m_commonException.getErrorData();
	}

	/* (non-Javadoc)
	 * @see com.ebay.kernel.exception.GenericException#getErrorDataStack()
	 */
	public List getErrorDataStack()
	{
		return m_commonException.getErrorDataStack();
	}

	/* (non-Javadoc)
	 * @see com.ebay.kernel.exception.GenericException#getStackTraceX()
	 */
	public String getStackTraceX()
	{
		return m_commonException.getStackTraceX();
	}
	
	public void superPrintStackTrace(PrintStream out)
	{
		super.printStackTrace(out);
	}
	

}
