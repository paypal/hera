package com.paypal.integ.odak.exp;

public class InitializationException extends RuntimeException {

	private static final long serialVersionUID = -7521691063106234095L;

	public InitializationException(String message) {
		super(message);
	}

	public InitializationException(String message, Throwable cause) {
		super(message, cause);
	}

}
