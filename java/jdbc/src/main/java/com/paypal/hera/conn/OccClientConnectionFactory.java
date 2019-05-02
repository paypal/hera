package com.paypal.hera.conn;

import java.util.Properties;

import com.paypal.hera.ex.OccConfigException;
import com.paypal.hera.ex.OccIOException;

public interface OccClientConnectionFactory {

	OccClientConnection createClientConnection(Properties props, String host, String port) throws OccIOException, OccConfigException ;
}
