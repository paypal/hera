package com.paypal.jmux.conf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.paypal.jmux.ex.OccConfigException;

public class OccConnectionConfig extends BaseOccConfiguration {
	
	static final int MAX_BUFFER_SIZE = 64 * 1024;
	static final int MAX_TIMEOUT = 3600000; // 1 hour in milliseconds
	
	public static final String OCC_SERVER_IP_PROPERTY = "occ.server.ip" ;
	
	public static final String OCC_SERVER_PORT_PROPERTY = "occ.server.port";
	// OCC client properties
	public static final String CONNECTION_RETRIES_PROPERTY = "occ.connection.retries";
	public static final String CONNECTION_TIMEOUT_MSECS_PROPERTY = "occ.connection.timeout.msecs";
	public static final String SO_SENDBUFFER_PROPERTY = "occ.socket.sendbuffer";
	public static final String SO_RECEIVEBUFFER_PROPERTY = "occ.socket.receivebuffer";
	public static final String SO_TIMEOUT_PROPERTY = "occ.socket.timeout";
	public static final String TCP_NO_DELAY_PROPERTY = "occ.socket.tcpnodelay";
	
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
	
	
	public OccConnectionConfig(Properties props, String host, String port) throws OccConfigException {

		super(props);
		this.config.setProperty(OCC_SERVER_IP_PROPERTY, host);
		this.config.setProperty(OCC_SERVER_PORT_PROPERTY, port);
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
	private String getValidatedIpAddress() throws OccConfigException {
		String host = config.getProperty(OCC_SERVER_IP_PROPERTY);
		if (host == null)
			throw new OccConfigException("Missing OCC configuration value for server ip");
		try {
			InetAddress.getByName(host);
			return host;
		} catch (UnknownHostException e) {
			throw new OccConfigException("Unable to look up the given host, " + host , e);
		}
	}
	
	private String getValidatedPort() throws OccConfigException {
		String portStr = config.getProperty(OCC_SERVER_PORT_PROPERTY);
		if (portStr == null)
			throw new OccConfigException("Missing OCC configuration value for server port");
		try {
			Integer.parseInt(portStr); //NOSONAR
			return portStr;
		} catch (NumberFormatException e) {
			throw new OccConfigException("Unable to parse the server port, " + portStr, e);
		}
	}

	public final String getIpAddress() throws OccConfigException {
		return hostIp;
	}

	public final String getPort() throws OccConfigException {
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
