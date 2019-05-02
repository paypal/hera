package com.paypal.jmux.client;

import com.paypal.jmux.conf.OCCClientConfigHolder;
import com.paypal.jmux.conn.OccClientConnection;
import com.paypal.jmux.conn.OccClientConnectionFactory;
import com.paypal.jmux.ex.OccExceptionBase;

public class OccClientFactory {
	public static OccClient createClient(OCCClientConfigHolder config, String host, String port) throws OccExceptionBase {	 
		
		
		OccClientConnectionFactory factory = config.getConnectionFactory();
		OccClientConnection conn = factory.createClientConnection(config.getProperties(), host, port);
		
		return new OccClientImpl(conn, config.getResponseTimeoutMs(), config.getSupportColumnNames(), config.getSupportColumnInfo());
	}
}
