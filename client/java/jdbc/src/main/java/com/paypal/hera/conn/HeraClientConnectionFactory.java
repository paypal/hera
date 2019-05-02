package com.paypal.hera.conn;

import java.util.Properties;

import com.paypal.hera.ex.HeraConfigException;
import com.paypal.hera.ex.HeraIOException;

public interface HeraClientConnectionFactory {

	HeraClientConnection createClientConnection(Properties props, String host, String port) throws HeraIOException, HeraConfigException ;
}
