package com.paypal.hera.ex;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class HeraExceptionBase extends SQLException {

	public HeraExceptionBase(String message) {
		super(message);
	}

	public HeraExceptionBase(Throwable cause) {
		super(cause);
	}

	public HeraExceptionBase(String string, Throwable cause) {
		super(string, cause);
	}
	
	public HeraExceptionBase(String message, String sqlState) {
	    super(message, sqlState);
	}    

	public HeraExceptionBase(String message, String sqlState, int code) {
	    super(message, sqlState, code);
	}    

}
