package com.paypal.hera.ex;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class OccRuntimeException extends OccExceptionBase {
	public OccRuntimeException(String message) {
		super(message);
	}

	public OccRuntimeException(SQLException e) {
		super(e.getMessage());
	}

}
