package com.paypal.hera.dal.jdbc.rt;

import com.paypal.hera.jdbc.HeraDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcDriverAdapterFactory {

	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on 09/02/05
	private static final ArrayList s_urlTable = new ArrayList();
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on 09/02/05
	private static final ArrayList s_driverTable = new ArrayList();
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on 09/02/05
	private static final ArrayList s_allAdapters = new ArrayList();
	
	private static Logger s_loggerCache;
	
	public static final String POOL_FACTORY = "PoolFactory";
	
	/* in order to make CmInitializer.java depending on this class/package
	 * and this class not knowing any attributes(name) which used to be in cminit.properties
	 * such as "PoolFactory" "DefaultPoolAdapter" and a new Attribute "DsLocation"
	 * this class need to hold this properties and let CmInitilizzer access it.
	 * 
	 */
	private static Properties s_props;
	public static Properties getCmInitProperties() {
		return s_props;
	}


	//TODO:HERA
//	public static JdbcDriverAdapter getAdapter(ConnectionPoolConfig poolConfig)
//	{
//		String dsName = poolConfig.getDataSourceName();
//		String url = poolConfig.getJdbcURL();
//		return getAdapter(dsName, url);
//	}


	public static void initAdapter() {
		JdbcDriverAdapter jdbcDriverAdapter = new HeraJDBCDriverAdapter();
		JdbcDriverAdapterEntryStr jdbcDriverAdapterEntryStr = new JdbcDriverAdapterEntryStr(
				HeraDriver.class.getName(), jdbcDriverAdapter
		);
		s_driverTable.add(jdbcDriverAdapterEntryStr);
	}
	public static JdbcDriverAdapter getAdapter(String dsName, String url)
	{
		JdbcDriverAdapter result = findAdapterForUrl(url);
		if (result != null) {
			return result;
		}

		// Use the DriverManager to find the first registered driver that 
		// understands this URL.  This seems like the best we can do with the
		// amount of data we have
		Object driver = null;
		try {
			driver = DriverManager.getDriver(url);
		} catch (SQLException sqle) {
			// throw runtime exception... no registered driver
			throw new RuntimeException("NO_REGISTERED_DRIVER_FOR_DATA_HOST", sqle);
		}

		// Look up driver class name to find adapter
		String driverClassName = driver.getClass().getName();
		result = findAdapterForDriver(driverClassName);

		// If no delegate was found, that's an error
		if (result == null) {
			// throw runtime exception
			throw new RuntimeException("NO_JDBC_ADAPTER_FOR_DRIVER");
		}

		return result;
	}

	private static JdbcDriverAdapter findAdapterForUrl(String url)
	{
		// Loop through the table and find the first matching adapter
		for (int i = 0; i < s_urlTable.size(); i++) {
			JdbcDriverAdapterEntryStr entry =
				(JdbcDriverAdapterEntryStr)s_urlTable.get(i);
			if (url.startsWith(entry.m_str)) {
				return entry.m_adapter;
			}
		}

		return null;
	}

	public static JdbcDriverAdapter findAdapterForDriver(
			String driverClassName)
	{
		// Loop through the table and find the first matching adapter
		for (int i = 0; i < s_driverTable.size(); i++) {
			JdbcDriverAdapterEntryStr entry =
				(JdbcDriverAdapterEntryStr)s_driverTable.get(i);
			if (driverClassName.startsWith(entry.m_str)) {
				return entry.m_adapter;
			}
		}

		return null;
	}

	private static class JdbcDriverAdapterEntryStr {
		private final String m_str;
		private final JdbcDriverAdapter m_adapter;

		JdbcDriverAdapterEntryStr(String str,
			JdbcDriverAdapter adapter)
		{
			m_str = str;
			m_adapter = adapter;
		}
	}

	private static void addStrEntries(List targetList,
		List prefixes, JdbcDriverAdapter adapter)
	{
		for (int i=0; i<prefixes.size(); i++) {
			String prefix = (String)prefixes.get(i);
			JdbcDriverAdapterEntryStr entry =
				new JdbcDriverAdapterEntryStr(prefix, adapter);
			targetList.add(entry);
		}
	}



	private static class JdbcAdapterReg {
		final String m_className;
		final Properties m_props;

		JdbcAdapterReg(String className, Properties props) {
			m_className = className;
			m_props = props;
		}
	}
}
