package com.paypal.jmux.conn;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.jmux.conf.OccConnectionConfig;
import com.paypal.jmux.ex.OccConfigException;
import com.paypal.jmux.ex.OccIOException;

public class OccTLSConnectionFactory implements OccClientConnectionFactory {
	
	final static Logger LOGGER = LoggerFactory.getLogger(OccTLSConnectionFactory.class);
	
	public OccClientConnection createClientConnection(Properties props, String host, String port) throws OccIOException, OccConfigException {
		OccConnectionConfig config = new OccConnectionConfig(props, host, port);
		try {
			return new OccTLSConnection(config);
		} catch (Exception e) {
			throw new OccIOException(e);
		}
	}
}
