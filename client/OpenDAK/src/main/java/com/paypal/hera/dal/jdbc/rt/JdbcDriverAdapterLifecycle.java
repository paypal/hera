package com.paypal.hera.dal.jdbc.rt;

import java.util.List;
import java.util.Properties;

/**
 * This interface has to be implemented by all registered
 * JDBC driver adapter classes
 * 
 */
public interface JdbcDriverAdapterLifecycle {

	/**
	 * Called to initialize the adapter
	 */
	void initialize(Properties props);

	/**
	 * Called to get list of driver class name prefixes
	 * 
	 * This is needed in order to find appropriate adapter for a connection
	 */
	List getDriverClassPrefixes();

	/**
	 * Called to get list of driver URL prefixes
	 * 
	 * This is needed in order to find appropriate adapter for a connection
	 */
	List getDriverUrlPrefixes();

	/**
	 * Returns a list of JDBC driver class names to initialize
	 */
	List getDriverClassNamesToInit();
}
