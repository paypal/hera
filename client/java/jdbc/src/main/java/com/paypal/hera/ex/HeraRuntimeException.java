package com.paypal.hera.ex;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class HeraRuntimeException extends HeraExceptionBase {
	public HeraRuntimeException(String message) {
		super(message);
	}

	public HeraRuntimeException(SQLException e) {
		super(e.getMessage());
	}

}
