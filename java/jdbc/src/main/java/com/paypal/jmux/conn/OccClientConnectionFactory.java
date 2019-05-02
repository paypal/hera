package com.paypal.jmux.conn;

import java.util.Properties;

import com.paypal.jmux.ex.OccConfigException;
import com.paypal.jmux.ex.OccIOException;

public interface OccClientConnectionFactory {

	OccClientConnection createClientConnection(Properties props, String host, String port) throws OccIOException, OccConfigException ;
}
