package com.paypal.integ.odak;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.paypal.hera.cal.CalEventHelper;
import com.paypal.integ.odak.exp.InitializationException;
import com.paypal.integ.odak.exp.OdakConfigException;

public class OdakConfigManager {

	private final static Logger logger = LoggerFactory.getLogger(OdakConfigManager.class);
	private final static OdakConfigManager INSTANCE = new OdakConfigManager();
	private Map<String, PoolConfig> configs = new ConcurrentHashMap<>();
	static String DALCP = "DALCP";
	static String ODAKCP = "ODAKCP";
	static String RCS_PROJECT_NAME_ODAK = "odak";
	static String RCS_CONFIG_NAME_INIT_CONFIG = "init-config";
	static String RCS_VERSION = "1.0.0";
	static String RCS_KEY_DEFAULT_CONNECTIONPOOL  = "CP-DEFAULT";
	static String RCS_KEY_WL_ODAK_POOL_NAMES  = "WL-ODAK-POOL-NAMES";
	static String RCS_KEY_WL_ODAK_BOX_NAMES = "WL-ODAK-BOX-NAMES";
	static String RCS_KEY_WL_DCP_POOL_NAMES = "WL-DCP-POOL-NAMES";
	static String RCS_KEY_WL_DCP_BOX_NAMES = "WL-DCP-BOX-NAMES";




	private OdakConfigManager() {
	}

	public static OdakConfigManager getInstance() {
		return INSTANCE;
	}

	Set<String> getOcpPoolNames() {
		return new HashSet<>(configs.keySet());
	}

	boolean isOcpPoolConfigured() {
		if (configs.isEmpty()) {
			return false;
		}
		return true;
	}

	public boolean doesExist(String host) {
		PoolConfig config = configs.get(host);
		if (config != null) {
			return true;
		}
		return false;
	}

	public void addPoolConfig(String dataSourceName, PoolConfig config) {
		PoolConfig existingConfig = configs.putIfAbsent(dataSourceName, config);
		if (existingConfig != null) {
			throw new OdakConfigException(
					"Validation error: multiple configs for the same pool specified, or duplicate initialization");
		}
	}

	public PoolConfig getPoolConfig(String host) throws InitializationException {
		if (host == null) {
			throw new InitializationException("OCP dataSource name cannot be null");
		}
		PoolConfig config = configs.get(host);
		if (config == null) {
			// no lazy init
			throw new InitializationException(
					"OCP dataSource " + host + " was not configured during start up or" + " could not be initialized");
		}
		return config;
	}

	public void loadConfig() throws OdakConfigException {
		try {
			boolean odakEnabled = isODAKEnabled();
			//Always enabled
//			if (isNonDevEnv() && ! odakEnabled){
//				return;
//			}

			String dsImportFilePath;
			//TODO:HERA
			dsImportFilePath = "dsimport.xml";
			if (dsImportFilePath == null || dsImportFilePath.equals("")) {
				throw new OdakConfigException("DsImport.xml file path is empty.");
			}
			URL url = null;
			File dsImportXml;
			InputStream dsImportIs = null;
//			try {
				//TODO:HERA
				ClassLoader classLoader = getClass().getClassLoader();
				url = classLoader.getResource(dsImportFilePath);
				if (url == null) {
					throw new IllegalArgumentException("dsimport.xml file is not found!");
				}
				
				//url = new URL(dsImportFilePath);
				dsImportXml = new File(url.getFile());
//			} 
//			catch (MalformedURLException murle) {
//				dsImportXml = new File(dsImportFilePath);
//			}
			if (url != null && (url.getProtocol().equals("jar") || url.getProtocol().equals("bundleresource"))) {
				try {
					dsImportIs = url.openStream();
				} catch (Exception e) {
					throw new OdakConfigException("failed to open resource m_url=" + url);
				}
			} else {
				if (!dsImportXml.exists()) {
					throw new OdakConfigException("dsimport file does not exist at" + dsImportFilePath);
				}
			}

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = null;
			if (dsImportIs != null) {
				// the dsimport.xml is from a jar file
				doc = dBuilder.parse(dsImportIs);
				dsImportIs.close();
				dsImportIs = null;
			} else {
				doc = dBuilder.parse(dsImportXml);
			}
			doc.getDocumentElement().normalize();
			NodeList dsList = doc.getElementsByTagName("data-source");
			for (int i = 0; i < dsList.getLength(); i++) {
				Node ds = dsList.item(i);
				if (ds.getNodeType() == Node.ELEMENT_NODE) {
					Element dsElement = (Element) ds;
					if (dsElement.getAttribute("type").equals("ODAK") || odakEnabled) {
						String dataSourceName;
						dataSourceName = dsElement.getAttribute("name");
						if (dataSourceName.startsWith("_")) {
							dataSourceName = dataSourceName.substring(1, dataSourceName.length());
						}
						if (dataSourceName.startsWith("CORE_")) {
							dataSourceName = dataSourceName.substring(5, dataSourceName.length());
						}
						String msg = String.format("Config load for OCP data source: %s", dataSourceName);
						logger.info("ODAK_INIT - {}", msg);
						CalEventHelper.writeLog("ODAK_INIT", dataSourceName, msg, "0");
						Node cProps = dsElement.getElementsByTagName("config-properties").item(0);
						PoolConfig config = new PoolConfig();
						config.setHost(dataSourceName);
						if (cProps.getNodeType() == Node.ELEMENT_NODE) {
							Element cPropsEle = (Element) cProps;
							NodeList properties = cPropsEle.getElementsByTagName("property");
							// TODO: Only first four configs are required.
							// Rest is exposed for testing purpose only.
							// Remove or block them before general release.
							for (int j = 0; j < properties.getLength(); j++) {
								Element propsEle = ((Element) properties.item(j));
								String propName = propsEle.getAttribute("name");
								switch (propName) {
								case "driver_class":
									config.setDriverClazz(propsEle.getAttribute("value"));
									break;
								case "URL":
									config.setUrl(propsEle.getAttribute("value"));
									break;
								case "user":
									config.setUsername(propsEle.getAttribute("value"));
									break;
								case "password":
									config.setPassword(propsEle.getAttribute("value"));
									break;
								case "minConnections":
									config.setMinConnections(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "maxConnections":
									config.setMaxConnections(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "idleTimeout":
									config.setIdleTimeout(Long.parseLong(propsEle.getAttribute("value")) * 1000);
									break;
								case "hardRecycleInterval":
									config.setHardRecycle(Long.parseLong(propsEle.getAttribute("value")) * 1000);
									break;
								case "softRecycleInterval":
									config.setSoftRecycle(Long.parseLong(propsEle.getAttribute("value")) * 1000);
									break;
								case "orphanTimeout":
									config.setOrphanTimeout(Integer.parseInt(propsEle.getAttribute("value")) * 1000);
									break;
								case "recycleIntervalMaxPadding":
									config.setrPaddingEnd(Integer.parseInt(propsEle.getAttribute("value")) * 1000);
									break;
								case "poolSizeTrackingInterval":
									GroomerConfig.getInstance().setPoolSizeTrackingInterval(
											Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "poolResizeInterval":
									GroomerConfig.getInstance().setPoolResizeInterval(
											Integer.parseInt(propsEle.getAttribute("value")) * 1000);
									break;
								case "poolUpwardResizeInterval":
									GroomerConfig.getInstance().setPoolUpwardResizeInterval(
											Integer.parseInt(propsEle.getAttribute("value")) * 1000);
									break;
								case "poolExtraCapacity":
									config.setPoolExtraCapacity(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "poolExtraCapacityForAging":
									config.setPoolExtraCapacityForAging(
											Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "stateLogFilePath":
									GroomerConfig.getInstance().setStateLogFilePath((propsEle.getAttribute("value")));
									break;
								case "stateLogFrequency":
									GroomerConfig.getInstance()
											.setStateLogFrequency(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "groomInterval":
									GroomerConfig.getInstance()
											.setGroomInterval(Integer.parseInt(propsEle.getAttribute("value")) * 1000);
									break;
								case "isStateLogEnable":
									GroomerConfig.getInstance()
											.setStateLogEnable(Boolean.parseBoolean(propsEle.getAttribute("value")));
									break;
								case "bkgExecutorPoolSize":
									GroomerConfig.getInstance()
											.setBkgExecutorPoolSize(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "bkgThreadRestartAttempts":
									GroomerConfig.getInstance()
											.setBkgExecutorPoolSize(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								case "maxStartupTime":
									GroomerConfig.getInstance()
											.setMaxStartupTime(Integer.parseInt(propsEle.getAttribute("value")));
									break;
								}
							}
						}

						PoolConfig existingConfig = configs.putIfAbsent(dataSourceName, config);
						if (existingConfig != null) {
							throw new OdakConfigException(
									"Validation error: multiple configs for the same pool specified, or duplicate initialization");
						}
						config.validate();
						GroomerConfig.getInstance().validate();
						config.dump();
						GroomerConfig.getInstance().dump();
					}
				}
			}
		} catch (ParserConfigurationException e) {
			throw new OdakConfigException("dsimport parsing failed.");
		} catch (SAXException e) {
			throw new OdakConfigException("dsimport parsing failed.");
		} catch (IOException e) {
			throw new OdakConfigException("dsimport parsing failed.");
		}
//		TODO:HERA
//		catch (ConfigurationException e1) {
//			String msg = "DsImport parsing failed. Error while trying to read location for dsimport file";
//			logger.error(msg, e1);
//			throw new OdakConfigException(msg);
//		}
	}

//
//	 boolean isNonDevEnv() {
//		boolean result = true;
//		try {
//			String COS = AppBuildConfig.getInstance().getClassOfService();
//			if ( "dev".equalsIgnoreCase(COS) ){
//				result = false;
//			}
//		}catch (Exception ex){
//			logger.debug(ex.getMessage());
//		}
//		return result;
//
//	}



	boolean  isODAKEnabled(){
		//Remove RCS code
		boolean initOdakCP = true;

//		CalEvent calEvent = CalEventFactory.create("DALINIT");
//		ConfigDescriptor configDescriptor = new ConfigDescriptor.Builder()
//				.version(RCS_VERSION)
//				.project(RCS_PROJECT_NAME_ODAK)
//				.name(RCS_CONFIG_NAME_INIT_CONFIG)
//				.disableCombinedConfiguration()
//				.build();
//
//		Configuration cpInitConfig = ConfigProvider.of(configDescriptor, getClass().getClassLoader());
//		String cpDefault =	 cpInitConfig.getString(RCS_KEY_DEFAULT_CONNECTIONPOOL); // CP-DEFAULT - default connection pool key in RCS
//		calEvent.addData("rcscpdefault",cpDefault);
//		if (cpDefault == null ){
//			cpDefault = DALCP;
//			calEvent.addData("cpdefault",cpDefault);
//		}
//
//		String poolName = AppBuildConfig.getInstance().getCalPoolName();
//		calEvent.addData("poolName",poolName);
//		poolName = normalize(poolName);
//		String boxname = ServerContext.getHostName() ;
//		calEvent.addData("boxname",boxname);
//		boxname = normalize(boxname);
//
//		initOdakCP = resolveWhitelistKeys(cpDefault,calEvent, cpInitConfig,poolName,boxname);
//		calEvent.setStatus("0");
//		calEvent.completed();

		return initOdakCP;

	}

//	boolean resolveWhitelistKeys(String cpDefault,CalEvent calEvent, Configuration cpInitConfig,String poolName,String boxName ) {
//		boolean  initOdakCP = false;
//		if(cpDefault.equalsIgnoreCase(DALCP)){
//			initOdakCP = handleODAKWLRCSKeys(calEvent, cpInitConfig,poolName,boxName);
//		}else if (cpDefault.equalsIgnoreCase(ODAKCP)){
//			initOdakCP = handleDCPWLRCSKeys(calEvent, cpInitConfig,poolName,boxName);
//		}
//
//		if (initOdakCP) {
//			calEvent.setName("ODAK");
//		} else {
//			calEvent.setName("DCP");
//		}
//
//		return  initOdakCP;
//	}

//	boolean handleODAKWLRCSKeys(CalEvent calEvent, Configuration cpInitConfig, String poolName,String boxName ){
//		boolean  initOdakCP = false;
//		String wlOcp =	normalizeValuesFromRCS(cpInitConfig.getString(RCS_KEY_WL_ODAK_POOL_NAMES, "-"), calEvent , "wlocppoolnames");
//		String wlOcpBoxNames = normalizeValuesFromRCS(cpInitConfig.getString(RCS_KEY_WL_ODAK_BOX_NAMES, "-"),calEvent, "wlocpboxnames");
//
//		if (wlOcp.contains(poolName)) {
//			initOdakCP = true;
//			calEvent.addData("poolwlocp","T");
//		}
//
//		if (wlOcpBoxNames.contains(boxName)){
//			initOdakCP = true;
//			calEvent.addData("boxwlocp","T");
//		}
//		return initOdakCP;
//
//	}


//	boolean handleDCPWLRCSKeys(CalEvent calEvent,Configuration cpInitConfig, String poolName,String boxName){
//
//		boolean  initOdakCP = true;
//		String wlDcp =	normalizeValuesFromRCS(cpInitConfig.getString(RCS_KEY_WL_DCP_POOL_NAMES, "-"),calEvent, "wldcppoolnames");
//		String wlDcpBoxNames = normalizeValuesFromRCS(cpInitConfig.getString(RCS_KEY_WL_DCP_BOX_NAMES, "-"),calEvent, "wldcpboxnames");
//
//		if ( wlDcp.contains(poolName)) {
//			initOdakCP = false;
//			calEvent.addData("poolwldcp","T");
//		}
//
//		if ( wlDcpBoxNames.contains(boxName)) {
//			initOdakCP = false;
//			calEvent.addData("boxwldcp","T");
//		}
//
//		return initOdakCP;
//	}


//
//	String normalizeValuesFromRCS(String input, CalEvent calEvent, String dataKey) {
//		calEvent.addData(dataKey,input);
//		return ","+input.replaceAll("\\s+","")+",";
//	}
//
//	String normalize(String input) {
//		return ","+input.trim()+",";
//	}




}
