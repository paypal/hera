package com.paypal.jmux.ex;

import com.paypal.jmux.util.ConnectionMetaInfo;

import java.io.IOException;

@SuppressWarnings("serial")
public class OccIOException extends OccExceptionBase {

	public OccIOException(String _message) {
		super(_message);
	}
	public OccIOException(IOException _ex) {
		super(_ex);
	}
	public OccIOException(Exception _ex) {
		super(_ex);
	}

	public OccIOException(IOException _ex, ConnectionMetaInfo connectionMetaInfo) {
		super(_ex.getMessage() + connectionMetaInfo.toString() , _ex);
	}


}
