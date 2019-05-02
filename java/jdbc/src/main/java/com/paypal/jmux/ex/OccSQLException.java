package com.paypal.jmux.ex;

public class OccSQLException extends OccExceptionBase {
	private static final long serialVersionUID = 6594041140506886608L;

	public OccSQLException(String _message) {
		super(_message);
	}

	public OccSQLException(String string, Throwable e) {
		super(string, e);
	}
}
