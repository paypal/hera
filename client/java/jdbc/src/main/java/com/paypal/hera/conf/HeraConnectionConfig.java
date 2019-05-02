package com.paypal.hera.conf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.paypal.hera.ex.HeraConfigException;

public class HeraConnectionConfig extends BaseHeraConfiguration {
	
	static final int MAX_BUFFER_SIZE = 64 * 1024;
	static final int MAX_TIMEOUT = 3600000; // 1 hour in milliseconds
	
	public static final String HERA_SERVER_IP_PROPERTY = "hera.server.ip" ;
	
	public static final String HERA_SERVER_PORT_PROPERTY = "hera.server.port";
	// Hera client properties
	public static final String CONNECTION_RETRIES_PROPERTY = "hera.connection.retries";
	public static final String CONNECTION_TIMEOUT_MSECS_PROPERTY = "hera.connection.timeout.msecs";
	public static final String SO_SENDBUFFER_PROPERTY = "hera.socket.sendbuffer";
	public static final String SO_RECEIVEBUFFER_PROPERTY = "hera.socket.receivebuffer";
	public static final String SO_TIMEOUT_PROPERTY = "hera.socket.timeout";
	public static final String TCP_NO_DELAY_PROPERTY = "hera.socket.tcpnodelay";
	
	public static final int DEFAULT_SO_SEND_BUFFER_SIZE = 0;
	public static final int DEFAULT_SO_RECV_BUFFER_SIZE = 0;
	public static final boolean DEFAULT_TCP_NO_DELAY = true;
	public static final int DEFAULT_SO_TIMEOUT = 60000;
	public static final int DEFAULT_CONNECTION_TIMEOUT_MSECS = 7000;
	public static final int DEFAULT_CONNECTION_RETRIES = 1;
	
	
	private Integer connectionTimeoutMs = 0;
	private Integer retries;
	private Integer socketSendBufferSize;
	private Integer socketReceiveBufferSize;
	private Integer socketTimeout;
	private Boolean tcpNoDelay;	
	private String hostIp;
	private String hostPort;
	private String host;
	
	
	public HeraConnectionConfig(Properties props, String host, String port) throws HeraConfigException {

		super(props);
		this.config.setProperty(HERA_SERVER_IP_PROPERTY, host);
		this.config.setProperty(HERA_SERVER_PORT_PROPERTY, port);
		retries = validateAndReturnDefaultInt(CONNECTION_RETRIES_PROPERTY, 0, Integer.MAX_VALUE, DEFAULT_CONNECTION_RETRIES);
		socketSendBufferSize = validateAndReturnDefaultInt(SO_SENDBUFFER_PROPERTY, 0, MAX_BUFFER_SIZE, DEFAULT_SO_SEND_BUFFER_SIZE);
		socketReceiveBufferSize = validateAndReturnDefaultInt(SO_RECEIVEBUFFER_PROPERTY, 0, MAX_BUFFER_SIZE, DEFAULT_SO_RECV_BUFFER_SIZE);
		socketTimeout = validateAndReturnDefaultInt(SO_TIMEOUT_PROPERTY, 0, Integer.MAX_VALUE, DEFAULT_SO_TIMEOUT);
		tcpNoDelay = validateAndReturnDefaultBoolean(TCP_NO_DELAY_PROPERTY, DEFAULT_TCP_NO_DELAY);
		connectionTimeoutMs = validateAndReturnDefaultInt(CONNECTION_TIMEOUT_MSECS_PROPERTY, 0, MAX_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_MSECS);
		hostIp = getValidatedIpAddress();
		hostPort = getValidatedPort();
		this.host = hostIp.toString() + ":" + hostPort;	
	}
	private String getValidatedIpAddress() throws HeraConfigException {
		String host = config.getProperty(HERA_SERVER_IP_PROPERTY);
		if (host == null)
			throw new HeraConfigException("Missing Hera configuration value for server ip");
		try {
			InetAddress.getByName(host);
			return host;
		} catch (UnknownHostException e) {
			throw new HeraConfigException("Unable to look up the given host, " + host , e);
		}
	}
	
	private String getValidatedPort() throws HeraConfigException {
		String portStr = config.getProperty(HERA_SERVER_PORT_PROPERTY);
		if (portStr == null)
			throw new HeraConfigException("Missing Hera configuration value for server port");
		try {
			Integer.parseInt(portStr); //NOSONAR
			return portStr;
		} catch (NumberFormatException e) {
			throw new HeraConfigException("Unable to parse the server port, " + portStr, e);
		}
	}

	public final String getIpAddress() throws HeraConfigException {
		return hostIp;
	}

	public final String getPort() throws HeraConfigException {
		return hostPort;
	}

	public final Integer getConnectionTimeoutMsecs() {
		return connectionTimeoutMs;
	}

	public final Integer getRetries() {
		return retries;
	}

	public final Integer getSocketSendBufferSize() {
		return socketSendBufferSize;
	}

	public final Integer getSocketReceiveBufferSize() {
		return socketReceiveBufferSize;
	}

	public final Integer getSocketTimeout() {
		return socketTimeout;
	}
	
	public final Boolean getTcpNoDelay() {
		return tcpNoDelay;
	}
	
	public final String getHost() {
		return host;
	}

}
