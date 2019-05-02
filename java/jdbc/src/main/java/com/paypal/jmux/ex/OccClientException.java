package com.paypal.jmux.ex;

public class OccClientException extends OccExceptionBase {
	private static final long serialVersionUID = -7150993755256340562L;

	public OccClientException(String _message) {
		super(_message);
	}

	public OccClientException(String string, Throwable cause) {
		super(string, cause);
	}
	
	public OccClientException(String message, String sqlState) {
	    super(message, sqlState);
	}   
	
	public OccClientException(String message, String sqlState, int code) {
	    super(message, sqlState, code);
	}    

}
