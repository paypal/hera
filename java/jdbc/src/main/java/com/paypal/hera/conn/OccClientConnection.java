package com.paypal.hera.conn;

import java.io.InputStream;
import java.io.OutputStream;

import com.paypal.hera.ex.OccIOException;

public interface OccClientConnection {
	public OutputStream getOutputStream();
	public InputStream getInputStream();
	public void close() throws OccIOException;
}
