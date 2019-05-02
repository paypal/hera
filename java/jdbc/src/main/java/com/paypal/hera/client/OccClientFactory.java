package com.paypal.hera.client;

import com.paypal.hera.conf.OCCClientConfigHolder;
import com.paypal.hera.conn.OccClientConnection;
import com.paypal.hera.conn.OccClientConnectionFactory;
import com.paypal.hera.ex.OccExceptionBase;

public class OccClientFactory {
	public static OccClient createClient(OCCClientConfigHolder config, String host, String port) throws OccExceptionBase {	 
		
		
		OccClientConnectionFactory factory = config.getConnectionFactory();
		OccClientConnection conn = factory.createClientConnection(config.getProperties(), host, port);
		
		return new OccClientImpl(conn, config.getResponseTimeoutMs(), config.getSupportColumnNames(), config.getSupportColumnInfo());
	}
}
