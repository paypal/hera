package com.paypal.hera.ex;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class OccExceptionBase extends SQLException {

	public OccExceptionBase(String message) {
		super(message);
	}

	public OccExceptionBase(Throwable cause) {
		super(cause);
	}

	public OccExceptionBase(String string, Throwable cause) {
		super(string, cause);
	}
	
	public OccExceptionBase(String message, String sqlState) {
	    super(message, sqlState);
	}    

	public OccExceptionBase(String message, String sqlState, int code) {
	    super(message, sqlState, code);
	}    

}
