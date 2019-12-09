package com.paypal.hera.conn;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;

import com.paypal.hera.ex.HeraIOException;

public interface HeraClientConnection {
	public OutputStream getOutputStream();
	public InputStream getInputStream();
	public void close() throws HeraIOException;
	public int getSoTimeout() throws SocketException;
	public void setSoTimeout(int tmo) throws SocketException ;
}
