package com.paypal.hera.ex;

public class HeraSQLException extends HeraExceptionBase {
	private static final long serialVersionUID = 6594041140506886608L;

	public HeraSQLException(String _message) {
		super(_message);
	}

	public HeraSQLException(String string, Throwable e) {
		super(string, e);
	}
}
