package com.paypal.integ.odak.exp;

import java.sql.SQLException;

/* 
 * To improve error logging don't throw the raw exception when possible, but wrap in the meaningful custom exceptions.
 * 
 * Generic exception to capture any data store connect failure. 
 * Can Wrap other exceptions like SQLException.
 */
public class OdakDataStoreConnectException extends SQLException {

	private static final long serialVersionUID = 7553095509647497323L;

	public OdakDataStoreConnectException(String message) {
		super(message);
	}

	public OdakDataStoreConnectException(String message, Throwable cause) {
		super(message, cause);
	}

}
