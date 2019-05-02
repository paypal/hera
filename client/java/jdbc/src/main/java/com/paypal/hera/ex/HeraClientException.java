package com.paypal.hera.ex;

public class HeraClientException extends HeraExceptionBase {
	private static final long serialVersionUID = -7150993755256340562L;

	public HeraClientException(String _message) {
		super(_message);
	}

	public HeraClientException(String string, Throwable cause) {
		super(string, cause);
	}
	
	public HeraClientException(String message, String sqlState) {
	    super(message, sqlState);
	}   
	
	public HeraClientException(String message, String sqlState, int code) {
	    super(message, sqlState, code);
	}    

}
