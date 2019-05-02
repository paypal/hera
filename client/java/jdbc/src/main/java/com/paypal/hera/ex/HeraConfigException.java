package com.paypal.hera.ex;

public class HeraConfigException extends HeraExceptionBase {
	private static final long serialVersionUID = 276589649083879310L;

	public HeraConfigException(String _message) {
		super(_message);
	}

	public HeraConfigException(Throwable cause) {
		super(cause);
	}

	public HeraConfigException(String string, Throwable cause) {
		super(string, cause);
	}
}
