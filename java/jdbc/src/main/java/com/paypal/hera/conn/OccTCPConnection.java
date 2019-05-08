package com.paypal.hera.conn;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalTransaction;
import com.paypal.hera.cal.CalTransactionFactory;
import com.paypal.hera.conf.OccConnectionConfig;
import com.paypal.hera.ex.OccExceptionBase;
import com.paypal.hera.ex.OccIOException;

public class OccTCPConnection implements OccClientConnection {
	final static Logger LOGGER = LoggerFactory.getLogger(OccTCPConnection.class);

	private Socket socket;
	private OutputStream requestStream;
	private InputStream responseStream;
	
	public OccTCPConnection(OccConnectionConfig config) throws OccExceptionBase {
			int retries = 0;
			CalTransaction calTrans = null;
			String dsName = config.validateAndReturnDefaultString("dsName", "");
			String foreground = config.validateAndReturnDefaultString("fg", "");
			while (true) {
				try {
					calTrans = CalTransactionFactory.create("CONNECT");
					calTrans.setName(config.getHost());
					calTrans.addData("Connect_attempt", String.valueOf(retries+1));
					
					long startTime = System.nanoTime(); // before DNS lookup
					InetAddress ip = InetAddress.getByName(config.getIpAddress());
					ip = InetAddress.getByAddress(config.getIpAddress(), ip.getAddress());
					InetSocketAddress addr = new InetSocketAddress(ip, Integer.parseInt(config.getPort()));

					socket = new Socket();
					socket.setTcpNoDelay(config.getTcpNoDelay());
					if (config.getSocketSendBufferSize() > 0)
						socket.setSendBufferSize(config.getSocketSendBufferSize());
					if (config.getSocketReceiveBufferSize() > 0)
						socket.setReceiveBufferSize(config.getSocketReceiveBufferSize());
					if (config.getSocketTimeout() > 0)
						socket.setSoTimeout(config.getSocketTimeout());
					socket.connect(addr, config.getConnectionTimeoutMsecs());
						
					requestStream = socket.getOutputStream();
					responseStream = socket.getInputStream();
					
					calTrans.addData("dns_usec", String.valueOf((System.nanoTime()- startTime)/1000L));
					calTrans.addData("laddr", getLocalSocketAddress(socket.getLocalSocketAddress() )); 
					calTrans.addData("host", dsName);
					calTrans.addData("fg", foreground);
					calTrans.setStatus("0");
					calTrans.completed();
					break;
				} catch (IOException e){
					calTrans.setStatus(e);
					calTrans.completed();
					if (retries < config.getRetries()) {
						retries++;
						LOGGER.debug( "Fail to connect, retrying ... " + retries);
					} else {
						throw new OccIOException(e);
					}
				}
			}
		}

	
	public OutputStream getOutputStream() {
		return requestStream;
	}

	
	public InputStream getInputStream() {
		return responseStream;
	}

	
	public void close() throws OccIOException {
		try {
			socket.close();
		} catch (IOException e) {
			throw new OccIOException(e);
		}
	}
	
	private String getLocalSocketAddress(SocketAddress socketAddress) {
		if (socketAddress == null) {
			return "";
		}
		if (socketAddress.toString().startsWith("/")) {
			return socketAddress.toString().substring(1);
		}
		return socketAddress.toString();
	}

}
