package com.paypal.hera.conn;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.conf.OccConnectionConfig;
import com.paypal.hera.ex.OccConfigException;
import com.paypal.hera.ex.OccIOException;

public class OccTCPConnectionFactory implements OccClientConnectionFactory {
	
	final static Logger LOGGER = LoggerFactory.getLogger(OccTCPConnectionFactory.class);
	
	public OccClientConnection createClientConnection(Properties props, String host, String port) throws OccIOException, OccConfigException {
		OccConnectionConfig config = new OccConnectionConfig(props, host, port);
		try {
			return new OccTCPConnection(config);
		} catch (Exception e) {
			throw new OccIOException(e);
		}
	}
}
