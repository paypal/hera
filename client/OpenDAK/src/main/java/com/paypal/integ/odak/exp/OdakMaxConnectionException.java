package com.paypal.integ.odak.exp;

import java.sql.SQLException;

public class OdakMaxConnectionException extends SQLException {

	private static final long serialVersionUID = -930528015948901242L;

	public OdakMaxConnectionException(String message) {
		super(message);
	}

	public OdakMaxConnectionException(String message, Throwable cause) {
		super(message, cause);
	}

}
