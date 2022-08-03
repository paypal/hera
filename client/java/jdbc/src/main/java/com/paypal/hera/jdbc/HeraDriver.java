package com.paypal.hera.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.ex.HeraExceptionBase;


/**
 * Hera Driver implementation
 * 
 * Example URL: jdbc:hera:1:[ip]:[port]
 * 
 * @author pvoicu
 */
public class HeraDriver implements Driver {

	final static Logger LOGGER = LoggerFactory.getLogger(HeraDriver.class);
	// defines driver version. all components of the driver
	// MUST use this definition and not specific protocol version
	static final String DRIVER_NAME = "Hera Driver";

	private static final String URL_PREFIX = "jdbc:hera:";

	static final int DRIVER_MAJOR_VERSION;
	static final int DRIVER_MINOR_VERSION;
	
	// tells if for result set we have column info. Having column info cause performance
	// penalty for the convenience to use the names over indexes
	static final boolean PROP_COLUMN_INFO = false; 

	private static HeraDriver s_driverInstance = new HeraDriver();

	private static Map<String, Integer> queryProperties = new ConcurrentHashMap<>();
	private static volatile boolean queryPropertiesLoaded = false;

	public HeraDriver() {
	}

	public Connection connect(String url, Properties info)
		throws SQLException
	{
		if (!url.startsWith(URL_PREFIX)) {
			return null;
		}
		StringBuffer host_ip = new StringBuffer();
		StringBuffer host_port = new StringBuffer();
		parseURL(url, host_ip, host_port);
		prepareQueryProperties(info);
		return new HeraConnection(info, host_ip.toString(), host_port.toString(), url);
	}

	public static Map<String, Integer> getQueryProperties() {
		return queryProperties;
	}
	private static void prepareQueryProperties(Properties properties){
		if(!queryPropertiesLoaded) {
			queryPropertiesLoaded = true;
			Set<String> keys = properties.stringPropertyNames();
			for (String key : keys) {
				try {
					if (key.startsWith("hera.query.") && key.endsWith(".readTimeout")) {
						Integer value = Integer.parseInt(properties.getProperty(key));
						queryProperties.put(key.split("hera\\.query\\.")[1].split("\\.readTimeout")[0], value);
					}
				} catch (Exception ex) {
					LOGGER.warn("failed while read timeout for " + key + " Exception: " +
							ex.getMessage());
				}
			}
		}
	}
	private void parseURL(String url, StringBuffer host_ip, StringBuffer host_port) throws HeraExceptionBase
	{
		int posn = 0;
		StringTokenizer strTokenizer = new StringTokenizer(url, ":");
		while (strTokenizer.hasMoreTokens()) {
			String str = strTokenizer.nextToken();
			switch (posn) {
				case 0: //jdbc
					break;
				case 1: // hera
					break;
				case 2:
					// '1'
					break;
				case 3:
					host_ip.append(str);
					break;
				case 4:
					host_port.append(str);
					break;
				default:
					LOGGER.warn( "Unexpected url content at position: " + posn + ", url is " + url);
			}
			posn++;
		}

		if (posn < 3) {
			throw new HeraExceptionBase("Incomplete url content: " + url);
		}
	}

	/**
	 * Returns true if the driver thinks that it can open a connection
	 * to the given URL.  Typically drivers will return true if they
	 * understand the subprotocol specified in the URL and false if
	 * they don't.
	 *
	 * @param url the URL of the database
	 * @return true if this driver can connect to the given URL  
	 * @exception SQLException for a database access error
	 */
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(URL_PREFIX);
	}

	/**
	 * Gets information about the possible properties for this driver.
	 */
	public DriverPropertyInfo[] getPropertyInfo(String url,
		Properties info) throws SQLException
	{
		return new DriverPropertyInfo[0];
	}

	/**
	 * Gets the driver's major version number
	 */
	public int getMajorVersion() {
		return DRIVER_MAJOR_VERSION;
	}

	/**
	 * Gets the driver's minor version number
	 */
	public int getMinorVersion() {
		return DRIVER_MINOR_VERSION;
	}

	/**
	 * Reports whether this driver is a genuine JDBC
	 * COMPLIANT driver.
	 * A driver may only report true here if it passes the JDBC compliance
	 * tests; otherwise it is required to return false.
	 */
	public boolean jdbcCompliant() {
		return false;
	}


	static {
		DRIVER_MAJOR_VERSION = 1;
		DRIVER_MINOR_VERSION = 1;

		try {
			DriverManager.registerDriver(s_driverInstance);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private final void notSupported() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException("Not supported on Hera Driver");
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		notSupported();
		return null;
	}
}
