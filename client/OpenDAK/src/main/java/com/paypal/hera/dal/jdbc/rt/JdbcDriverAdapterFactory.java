package com.paypal.hera.dal.jdbc.rt;

import java.io.IOException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.paypal.hera.dal.DalConstants;
import com.paypal.hera.dal.DalFileComponentStatus;
import com.paypal.hera.dal.DalRuntimeException;
import com.paypal.integ.odak.exp.InitializationException;

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
	private static Logger getLogger() {
		if (s_loggerCache == null) {
			s_loggerCache = Logger.getInstance(
				JdkUtil.forceInit(JdbcDriverAdapterFactory.class));
		}

		return s_loggerCache;
	}

	
	public static void initialize(InitializationContext context) {
		//Properties props;
		try {
			URL propFile = ResourceUtil.getResource(DalConstants.DAL_CONFIG_FILE_DIR, "CmInit.properties");
			if (propFile != null)  {
				DalFileComponentStatus.getInstance().registerFile(propFile);
				s_props = new Properties();
				s_props.load(propFile.openStream());
			}
		} catch (IOException e) {
			throw new InitializationException(
				"Unable to load CmInit.properties - " + e.toString(), e);
		}

		if (s_props == null) {
			throw new InitializationException(
				"Unable to find CmInit.properties");
		}
		
		// we are removing the dbitdriver dependency, but there may be many CmInit.properties files that
		// reference it.  So what we'll do is to ignore that particular adapter and remove it from the loaded props.
		String dbitdriver = "jdbcadapter.dbit.class";
		if (s_props.containsKey(dbitdriver))  {
			getLogger().log(LogLevel.INFO,
					"Ignoring JDBC Driver: " + dbitdriver);
			s_props.remove(dbitdriver);
		}
		
		Map adapters = PropertiesUtils.getPropertyGroupsMapByPrefix(
			s_props, "jdbcadapter");

		List adapterRegs = new ArrayList(adapters.size());
		for (Iterator it=adapters.entrySet().iterator(); it.hasNext(); ) {
			Map.Entry e = (Map.Entry)it.next();
			String name = (String)e.getKey();
			Properties props2 = (Properties)e.getValue();
			String className = props2.getProperty("class");

			if (className != null) {
				className = className.trim();
			}

			if (className == null || className.length() == 0) {
				throw new InitializationException(
					"Class name is not specified for " + name +
					" in CmInit.properties");
			}

			Properties adapterProps = PropertiesUtils.
				getSubpropertiesByPrefix(props2, "props");

			adapterRegs.add(new JdbcAdapterReg(className, adapterProps));
		}

		initialize(adapterRegs, context);
	}

	private static void initialize(List adapterRegs,
		InitializationContext context)
	{
		for (int i=0; i<adapterRegs.size(); i++) {
			JdbcAdapterReg adapterReg = (JdbcAdapterReg)adapterRegs.get(i);
			initializeAdapter(adapterReg, context);
		}
	}

	private static void initializeAdapter(JdbcAdapterReg adapterReg,
		InitializationContext context)
	{
		String className = adapterReg.m_className;

		for (int i=0; i<s_allAdapters.size(); i++) {
			String existingClassName =
				s_allAdapters.get(i).getClass().getName();
			if (existingClassName.equals(className)) {
				return;
			}
		}

		JdbcDriverAdapterLifecycle adapter;
		try {
			Class clazz = Class.forName(className);
			adapter = (JdbcDriverAdapterLifecycle)clazz.newInstance();
		} catch (Exception e) {
			throw new InitializationException("Failed to instantiate " +
				className + ": " + e.toString(), e);
		}

		if (!(adapter instanceof JdbcDriverAdapter)) {
			throw new InitializationException(className +
				" must implement JdbcDriverAdapter interface");
		}

		JdbcDriverAdapter adapter2 = (JdbcDriverAdapter)adapter;

		/* isVerbose() may return "false" only when it is from batch jobs */
		if (BaseInitializationContext.isVerbose()) {
		    context.out("---------- Registering JDBC Driver Adapter: " +
			className + " ----------");
		}

		adapter.initialize(adapterReg.m_props);

		List driverPrefixes = adapter.getDriverClassPrefixes();
		List urlPrefixes = adapter.getDriverUrlPrefixes();
		List drivers = adapter.getDriverClassNamesToInit();

		for (int i=0; i<drivers.size(); i++) {
			String driverClassName = (String)drivers.get(i);
			registerJDBCDriver(driverClassName, context);
		}

		s_allAdapters.add(adapter2);
		addStrEntries(s_driverTable, driverPrefixes, adapter2);
		addStrEntries(s_urlTable, urlPrefixes, adapter2);
	}

	public static List getAllAdapters() {
		return new ArrayList(s_allAdapters);
	}

	//TODO:HERA
//	public static JdbcDriverAdapter getAdapter(ConnectionPoolConfig poolConfig)
//	{
//		String dsName = poolConfig.getDataSourceName();
//		String url = poolConfig.getJdbcURL();
//		return getAdapter(dsName, url);
//	}

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
			Message msg = new Message(
				"NO_REGISTERED_DRIVER_FOR_DATA_HOST",
				Args.with(dsName, url, sqle.getMessage()));
			throw new DalRuntimeException(msg, sqle);
		}

		// Look up driver class name to find adapter
		String driverClassName = driver.getClass().getName();
		result = findAdapterForDriver(driverClassName);

		// If no delegate was found, that's an error
		if (result == null) {
			// throw runtime exception
			Message msg = new Message(
				"NO_JDBC_ADAPTER_FOR_DRIVER",
				new String[] {driverClassName, dsName, url});
			throw new DalRuntimeException(msg);
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

	private static JdbcDriverAdapter findAdapterForDriver(
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

	/**
	 * Locate, load and link the JDBC Driver class to use for DAL database
	 * operations.
	 */
	private static void registerJDBCDriver(String className,
		InitializationContext context)
	{
		try {
			Class.forName(className);
			//context.out("---------- Loaded JDBC Driver: " +
			//	className + " ----------");
		} catch (Exception e) {
			context.out("---------- Failed to load JDBC Driver: " +
				className + " ----------");
			getLogger().log(LogLevel.ERROR,
				"Unable to load JDBC Driver: " + className, e);
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
