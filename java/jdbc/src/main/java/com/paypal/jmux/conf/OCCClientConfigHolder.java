package com.paypal.jmux.conf;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.jmux.conn.OccClientConnectionFactory;
import com.paypal.jmux.ex.OccConfigException;

public final class OCCClientConfigHolder extends BaseOccConfiguration {
	
	final Logger LOGGER = LoggerFactory.getLogger(OCCClientConfigHolder.class);
	
	private static HashMap<String, OCCClientConfigHolder > configs = new HashMap<String, OCCClientConfigHolder >();
		
	// property name definitions
	public static final String SUPPORT_COLUMN_NAMES_PROPERTY = "occ.support.column_names";
	public static final String SUPPORT_COLUMN_INFO_PROPERTY = "occ.support.column_info";
	public static final String SUPPORT_RS_METADATA_PROPERTY = "occ.support.rs_metadata";
	public static final String MIN_FETCH_SIZE_PROPERTY = "occ.min_fetch_size";
	public static final String CONNECTION_FACTORY_PROPERTY = "occ.connection.factory";
	public static final String RESPONSE_TIMEOUT_MS_PROPERTY = "occ.response.timeout.ms";
	public static final String ENABLE_ESCAPE_PROPERTY = "occ.enable.escape";
	public static final String ENABLE_SHARDING_PROPERTY = "occ.enable.sharding";
	public static final String ENABLE_BATCH_PROPERTY = "occ.enable.batch";
	public static final String ENABLE_PARAM_NAME_BINDING = "occ.enable.param_name_binding";
	public static final String DB_ENCODING_UTF8 = "occ.db_encoding.utf8";
	public static final String ENABLE_DATE_NULL_FIX = "occ.enable.date_null_fix"; // ! TODO: this should be cleaned-up after all OCC are rolled out with the server fix
	// defaults
	public static final boolean DEFAULT_SUPPORT_COLUMN_NAMES = true;
	public static final boolean DEFAULT_SUPPORT_COLUMN_INFO = true;
	public static final boolean DEFAULT_SUPPORT_RS_METADATA = true;
	public static final int DEFAULT_MIN_FETCH_SIZE = 2;
	public static final int DEFAULT_RESPONSE_TIMEOUT_MS = 60000;
	public static final boolean DEFAULT_ENABLE_ESCAPE = true;
	public static final boolean DEFAULT_ENABLE_SHARDING = false;
	public static final boolean DEFAULT_ENABLE_BATCH = false;
	public static final boolean DEFAULT_ENABLE_PARAM_NAME_BINDING = true;
	public static final boolean DEFAULT_DB_ENCODING_UTF8 = true;
	public static final boolean DEFAULT_ENABLE_DATE_NULL_FIX = false;
	
	public static final String DEFAULT_CONNECTION_FACTORY="com.paypal.jmux.conn.OccTCPConnectionFactory";
	//public static final String DEFAULT_CONNECTION_FACTORY="com.paypal.jmux.conn.OccTLSConnectionFactory";
	// mvn3 -Djavax.net.ssl.trustStore=src/test/resources/TlsClientKeystore.jks -Djavax.net.ssl.trustStorePassword=61-Moog

	
	private Boolean supportColumnNames;
	private Boolean supportColumnInfo;
	private Boolean supportRSMetadata;
	private Integer minFetchSize;
	private Integer responseTimeoutMs;
	private Boolean enableEscape;
	private Boolean enableSharding;
	private Boolean enableBatch;
	private Boolean enableParamNameBinding;
	private Boolean isDBEncodingUTF8;
	private Boolean enableDateNullFix;
	
	private OccClientConnectionFactory connectionFactory;

	public OCCClientConfigHolder(Properties props) throws OccConfigException, ClassNotFoundException, IllegalAccessException, InstantiationException {

		super(props);
		
		validateAndFillAll();	
		
	}

	private void validateAndFillAll() throws OccConfigException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		LOGGER.debug("Creating config");
		
		String connectionFactoryClassName = validateAndReturnDefaultString(CONNECTION_FACTORY_PROPERTY, DEFAULT_CONNECTION_FACTORY );
		ClassLoader loader = OCCClientConfigHolder.class.getClassLoader();
		if (loader == null) {
			throw new ClassNotFoundException();
		}
		this.connectionFactory = (OccClientConnectionFactory) loader.loadClass(connectionFactoryClassName).newInstance();
		if (this.connectionFactory == null) {
			throw new ClassNotFoundException();
		}
		
		supportColumnNames = validateAndReturnDefaultBoolean(SUPPORT_COLUMN_NAMES_PROPERTY, DEFAULT_SUPPORT_COLUMN_NAMES);
		supportColumnInfo = validateAndReturnDefaultBoolean(SUPPORT_COLUMN_INFO_PROPERTY, DEFAULT_SUPPORT_COLUMN_INFO);
		supportRSMetadata = validateAndReturnDefaultBoolean(SUPPORT_RS_METADATA_PROPERTY, DEFAULT_SUPPORT_RS_METADATA);
		minFetchSize = validateAndReturnDefaultInt(MIN_FETCH_SIZE_PROPERTY, 0, Integer.MAX_VALUE, DEFAULT_MIN_FETCH_SIZE);
		
		responseTimeoutMs = validateAndReturnDefaultInt(RESPONSE_TIMEOUT_MS_PROPERTY, 0, Integer.MAX_VALUE, DEFAULT_RESPONSE_TIMEOUT_MS);
		enableEscape = validateAndReturnDefaultBoolean(ENABLE_ESCAPE_PROPERTY, DEFAULT_ENABLE_ESCAPE);
		enableSharding = validateAndReturnDefaultBoolean(ENABLE_SHARDING_PROPERTY, DEFAULT_ENABLE_SHARDING);
		enableBatch = validateAndReturnDefaultBoolean(ENABLE_BATCH_PROPERTY, DEFAULT_ENABLE_BATCH);
		enableParamNameBinding = validateAndReturnDefaultBoolean(ENABLE_PARAM_NAME_BINDING, DEFAULT_ENABLE_PARAM_NAME_BINDING);
		isDBEncodingUTF8 = validateAndReturnDefaultBoolean(DB_ENCODING_UTF8, DEFAULT_DB_ENCODING_UTF8);
		enableDateNullFix = validateAndReturnDefaultBoolean(ENABLE_DATE_NULL_FIX, DEFAULT_ENABLE_DATE_NULL_FIX);
	}

	
	public Integer getResponseTimeoutMs() {
		return responseTimeoutMs;
	}
	
	public Boolean getSupportColumnNames() {
		return supportColumnNames;
	}
	
	public Boolean getSupportColumnInfo() {
		return supportColumnInfo;
	}
	
	public Boolean getSupportRSMetadata() {
		return supportRSMetadata;
	}


	public Integer getMinFetchSize() {
		return minFetchSize;
	}
	
	public boolean enableEscape() {
			return enableEscape;
	}
	
	public boolean enableSharding() {
		return enableSharding;
	}
	
	public boolean enableBatch() {
		return enableBatch;
	}
	
	public boolean enableParamNameBinding() {
		return enableParamNameBinding;
	}
	
	public boolean isDBEncodingUTF8() {
		return isDBEncodingUTF8;
	}
	
	public boolean enableDateNullFix() {
		return enableDateNullFix;
	}

	public Properties getProperties() {
		return this.config;
	}
	public OccClientConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}
	
	// used by tests to start with no cache
	synchronized public static void clear() {
		configs.clear();
	}

}
