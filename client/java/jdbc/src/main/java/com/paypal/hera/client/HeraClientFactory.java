package com.paypal.hera.client;

import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conn.HeraClientConnection;
import com.paypal.hera.conn.HeraClientConnectionFactory;
import com.paypal.hera.ex.HeraExceptionBase;

public class HeraClientFactory {
	public static HeraClient createClient(HeraClientConfigHolder config, String host, String port) throws HeraExceptionBase {	 
		
		
		HeraClientConnectionFactory factory = config.getConnectionFactory();
		HeraClientConnection conn = factory.createClientConnection(config.getProperties(), host, port);
		
		return new HeraClientImpl(conn, config.getResponseTimeoutMs(), config.getSupportColumnNames(), config.getSupportColumnInfo());
	}
}
