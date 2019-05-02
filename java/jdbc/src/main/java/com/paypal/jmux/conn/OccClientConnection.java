package com.paypal.jmux.conn;

import java.io.InputStream;
import java.io.OutputStream;
import com.paypal.jmux.ex.OccIOException;

public interface OccClientConnection {
	public OutputStream getOutputStream();
	public InputStream getInputStream();
	public void close() throws OccIOException;
}
