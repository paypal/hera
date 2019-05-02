package com.paypal.jmux.ex;

public class OccConfigException extends OccExceptionBase {
	private static final long serialVersionUID = 276589649083879310L;

	public OccConfigException(String _message) {
		super(_message);
	}

	public OccConfigException(Throwable cause) {
		super(cause);
	}

	public OccConfigException(String string, Throwable cause) {
		super(string, cause);
	}
}
