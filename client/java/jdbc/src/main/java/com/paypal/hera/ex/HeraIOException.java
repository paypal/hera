package com.paypal.hera.ex;

import java.io.IOException;

import com.paypal.hera.util.ConnectionMetaInfo;

@SuppressWarnings("serial")
public class HeraIOException extends HeraExceptionBase {

	public HeraIOException(String _message) {
		super(_message);
	}
	public HeraIOException(IOException _ex) {
		super(_ex);
	}
	public HeraIOException(Exception _ex) {
		super(_ex);
	}

	public HeraIOException(IOException _ex, ConnectionMetaInfo connectionMetaInfo) {
		super(_ex.getMessage() + connectionMetaInfo.toString() , _ex);
	}


}
