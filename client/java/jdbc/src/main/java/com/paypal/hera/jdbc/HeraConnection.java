package com.paypal.hera.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.client.HeraClient;
import com.paypal.hera.client.HeraClientFactory;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.conf.HeraClientConfigHolder.E_DATASOURCE_TYPE;
import com.paypal.hera.ex.HeraConfigException;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraRuntimeException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraStatementsCache;
import com.paypal.hera.util.HeraStatementsCachePool;

public class HeraConnection implements Connection {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraConnection.class);
	
	private boolean isClosed;
	private HeraClient heraClient = null;
	private HeraDatabaseMetadata metaData;
	private WeakHashMap<HeraStatement, Integer> statements;
	private HeraStatementsCache statementCache;
	private HeraResultSet active_rs = null;
	private boolean supportRSMetaData = false;
	private boolean autoCommit;
	private HeraJdbcConverter converter;
	private String server_name;
	private String url;
	private Integer minFetchSize;
	private boolean escapeEnabled;
	private boolean shardingEnabled;
	private boolean batchEnabled;
	private boolean paramNameBindingEnabled;
	private boolean isDBEncodingUTF8;
	private boolean dateNullFixEnabled;
	private E_DATASOURCE_TYPE datasource;
	private byte[] shardKeyPayload;
	private int shardID;
	private static final String SERVER_LOGICAL_NAME = "host";
	private static final String CAL_LOGGING_OPTIONS = "calLogOption";
	public static final String OCC_CLIENT_CONN_ID = "OccClientConnID";

	public HeraConnection(Properties _props, String _address, String _port, String _url) throws HeraIOException, HeraConfigException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("(id: " + this.hashCode() + "): " + "Hera Connecting to " + _address + ":" + _port);
		}
		url = _url;
		try {
			HeraClientConfigHolder config = new HeraClientConfigHolder(_props);

			// the driver custom outh not needed since netclient is doing it
			// Authenticator auth = new Authenticator(config.getCustomAuth(), config.getUsername(), config.getEncryptedAuthKey());
			// auth.authenticate(responseStream, requestStream);
			
			heraClient = HeraClientFactory.createClient(config, _address, _port);
			heraClient.setServerLogicalName(_props.getProperty(SERVER_LOGICAL_NAME));
			heraClient.setCalLogOption(_props.getProperty(CAL_LOGGING_OPTIONS));
			statements = new WeakHashMap<HeraStatement, Integer>();
			supportRSMetaData = config.getSupportRSMetadata();
			minFetchSize = config.getMinFetchSize();
			autoCommit = false; 
			converter = new HeraJdbcConverter();
			statementCache = HeraStatementsCachePool.getStatementsCache(_url);
			//heraClient.sendClientInfo("init", "");
			escapeEnabled = config.enableEscape();
			shardingEnabled = config.enableSharding();
			batchEnabled = config.enableBatch();
			paramNameBindingEnabled = config.enableParamNameBinding();
			isDBEncodingUTF8 = config.isDBEncodingUTF8();
			dateNullFixEnabled = config.enableDateNullFix();
			datasource = config.getDataSourceType();
			setShardKeyPayload(null);
			setShardID(-1);
			
			// extract server name
			String server_info = heraClient.sendClientInfo("init", "");
			int pos = server_info.indexOf(":");
			if (pos == -1)
				server_name = "unknown_";
			else
				server_name = server_info.substring(0, pos) + "_";
			pos = server_info.indexOf("Host=");
			if (pos == -1)
				server_name += "unknown";
			else
				server_name += server_info.substring(pos + "Host=".length());

			heraClient.setHeraHostName(server_name);
						
		} catch (HeraConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new HeraIOException(e);
		}
	}

	void unregister(HeraStatement _st) {
		if (!isClosed)
			statements.remove(_st);
	}
	
	void checkOpened() throws HeraRuntimeException {
		if (isClosed)
			throw new HeraRuntimeException("Connection is closed");
	}

	/* used for testing only - for the manual leak detection test
	public int statementsCount() {
		return statements.size();
	}
	*/
	
	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	private void cleanup() throws HeraExceptionBase {
		for (HeraStatement st: statements.keySet()) {
			st.close();
		}
		statements.clear();
		statements = null;
		HeraStatementsCachePool.releaseStatementsCache(url, statementCache);
	}
	
	public void hardClose() throws HeraExceptionBase {
		close();
	}
	
	@Override
	public void close() throws HeraExceptionBase {
		if (isClosed)
			return;
		
		isClosed = true;
		cleanup();
		try {
			heraClient.close();
		} catch (Exception e) {
			LOGGER.error("Fail to release transport");
			throw new HeraIOException(e);
		}
	}
	
	public Statement createStatement() throws SQLException {
		checkOpened();
		HeraStatement st = new HeraStatement(this);
		statements.put(st, 1);
		return st;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkOpened();
		HeraPreparedStatement st = new HeraPreparedStatement(this, sql);
		statements.put(st, 1);
		return st;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		checkOpened();
		HeraCallableStatement st = new HeraCallableStatement(this, sql);
		statements.put(st, 1);
		return st;
	}

	private void checkStatementOptions(int resultSetType,
		int resultSetConcurrency) throws SQLException
	{
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new HeraSQLException("Hera resultset must be forward-only");
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new HeraSQLException("Hera resultset must be read-only");
		}
	}

	public Statement createStatement(
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		checkStatementOptions(resultSetType, resultSetConcurrency);		
		return createStatement();
	}

	public PreparedStatement prepareStatement(String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		checkStatementOptions(resultSetType, resultSetConcurrency);		
		return prepareStatement(sql);
	}

	public CallableStatement prepareCall(String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		checkStatementOptions(resultSetType, resultSetConcurrency);		
		return prepareCall(sql);
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		checkOpened();
		if (metaData == null) {
			metaData = new HeraDatabaseMetadata(this);
		}
		return metaData;
	}

	public String nativeSQL(String sql) throws SQLException {
		checkOpened();
		return sql;
	}

	public void setAutoCommit(boolean _autoCommit) throws SQLException {
		checkOpened();
		autoCommit = _autoCommit;
	}

	public boolean getAutoCommit() throws SQLException {
		checkOpened();
		return autoCommit;
	}

	public void setTransactionIsolation(int level) throws SQLException {
		checkOpened();
		if (level != TRANSACTION_READ_COMMITTED) {
			throw new HeraSQLException("Unsupported transactions isolation level " + level);
		}
	}

	public int getTransactionIsolation() throws SQLException {
		checkOpened();
		return TRANSACTION_READ_COMMITTED;
	}

	public void commit() throws SQLException {
		checkOpened();
		try {
			getHeraClient().commit();
		} catch(HeraIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public void rollback() throws SQLException {
		checkOpened();
		try {
			getHeraClient().rollback();
		} catch(HeraIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		checkOpened();
		if (readOnly) {
			throw new HeraSQLException("Hera driver does not support " + "read-only transactions");
		}
	}

	public boolean isReadOnly() throws SQLException {
		checkOpened();
		return false;
	}

	public void setCatalog(String catalog) throws SQLException {
		checkOpened();
		// ignoring per JDBC spec
	}

	public String getCatalog() throws SQLException {
		checkOpened();
		return null;
	}

	public SQLWarning getWarnings() throws SQLException {
		checkOpened();
		return null;
	}

	public void clearWarnings() throws SQLException {
		checkOpened();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Map getTypeMap() throws SQLException {
		checkOpened();
		return Collections.EMPTY_MAP;
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		checkOpened();
		throw new HeraSQLException("Custom type maps are not supported by Hera");
	}

	// JDBC3.0 COMPATIBILITY

	private void notSupported() throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("Not supported on Hera connection");
	}

	public void setHoldability(int holdability) throws SQLException {
		notSupported();
	}

	public int getHoldability() throws SQLException {
		notSupported();
		return 0;
	}

	public Savepoint setSavepoint() throws SQLException {
		notSupported();
		return null;
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		notSupported();
		return null;
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		notSupported();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		notSupported();
	}

	public Statement createStatement(int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		notSupported();
		return null;
	}

	public PreparedStatement prepareStatement(String sql,
		int resultSetType, int resultSetConcurrency,
		int resultSetHoldability) throws SQLException
	{
		notSupported();
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		notSupported();
		return null;
	}

	public PreparedStatement prepareStatement(String sql,
		int autoGeneratedKeys) throws SQLException
	{
		HeraCallableStatement out = (HeraCallableStatement)prepareCall(sql);
		if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
			out.registerOutParameter(5000, java.sql.Types.NUMERIC);
		}
		return out;
	}

	public PreparedStatement prepareStatement(String sql,
		int[] columnIndexes) throws SQLException
	{
		notSupported();
		return null;
	}

	public PreparedStatement prepareStatement(String sql,
		String[] columnNames) throws SQLException
	{
		notSupported();
		return null;
	}

	// JDBC 4.0 Compatibility
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		notSupported();
		return null;
	}

	public Blob createBlob() throws SQLException {
		return new HeraBlob();
	}

	public Clob createClob() throws SQLException {
		return new HeraClob();
	}

	public NClob createNClob() throws SQLException {
		notSupported();
		return null;
	}

	public SQLXML createSQLXML() throws SQLException {
		notSupported();
		return null;
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		notSupported();
		return null;
	}

	public Properties getClientInfo() throws SQLException {
		notSupported();
		return null;
	}

	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.getClientInfo is not implemented");
	}

	public boolean isValid(int timeout) throws SQLException {
		if (isClosed()) {
			return false;
		}
		if (timeout < 0) { 
			throw new SQLException("Timeout must be positive");
		}
		try {
			getHeraClient().ping(timeout * 1000);
			return true;
		} catch(Exception e) {
			close();
			return false;
		}
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			notSupported();
		} catch (SQLException e) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("setClientInfo ex: " + e.getMessage());
			}
			Map<String, ClientInfoStatus> failedProperties = new TreeMap<String, ClientInfoStatus>();
			for (Object key: properties.keySet()){
				failedProperties.put(key.toString(), null);
			}
			throw new SQLClientInfoException("NotImplemented", failedProperties);
		}
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			notSupported();
		} catch (SQLException e) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("setClientInfo ex: " + e.getMessage());
			}
			Map<String, ClientInfoStatus> failedProperties = new TreeMap<String, ClientInfoStatus>();
			failedProperties.put(name, null);
			throw new SQLClientInfoException("NotImplemented", failedProperties);
		}
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.isWrapperFor is not implemented");
		}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		notSupported();
		return null;
	}

	public HeraClient getHeraClient() {
		return heraClient;
	}

	public void setActiveResultSet(HeraResultSet active_rs) {
		this.active_rs = active_rs;
	}

	public HeraResultSet getActiveResultSet() {
		return active_rs;
	}

	public boolean supportResultSetMetaData() {
		return supportRSMetaData;
	}

	public HeraStatementsCache getStatementCache() {
		return statementCache;
	}

	public String getUrl() {
		return url;
	}

	public HeraJdbcConverter getConverter() {
		return converter;
	}

	public Integer getMinFetchSize() {
		return minFetchSize;
	}

	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.setSchema is not implemented");
		}

	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.getSchema is not implemented");
		}

	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.abort is not implemented");
		}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.setNetworkTimeout is not implemented");
		}

	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraConnection.getNetworkTimeout is not implemented");
		}

	
	public String getServerName() {
		return server_name;
	}

	public boolean enableEscape() {
		return escapeEnabled;
	}
	
	public boolean shardingEnabled() {
		return shardingEnabled;
	}
	
	public boolean batchEnabled() {
		return batchEnabled;
	}
	
	public boolean paramNameBindingEnabled() {
		return paramNameBindingEnabled;
	}
	
	public boolean  isDBEncodingUTF8() {
		return isDBEncodingUTF8;
	}
	
	public int getShardCount() throws SQLException {
		checkOpened();
		if (!shardingEnabled())
			return 1;
		try {
			return getHeraClient().getNumShards();
		} catch(HeraIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public void setShardHint(String _key, String _value) throws SQLException {
		checkOpened();
		if (!shardingEnabled())
			return;
		try {
			if (_key.equals("shardid")) {
				setShardID(Integer.parseInt(_value));
				getHeraClient().setShard(getShardID());
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("(id: " + this.hashCode() + "): " + "setShardHint(" + _key + ")");
				}
				setShardKeyPayload((_key + "=" + _value).getBytes());
			}
		} catch(Exception ex) {
			hardClose();
			throw ex;
		}
	}
	
	public void resetShardHints() throws SQLException {
		if (!shardingEnabled())
			return;
		checkOpened();
		try {
			if (getShardID() != -1) {
				setShardID(-1);
				getHeraClient().setShard(-1);
			}
			setShardKeyPayload(null);
		} catch(HeraIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public byte[] getShardKeyPayload() {
		return shardKeyPayload;
	}

	public final void setShardKeyPayload(byte[] shardKeyPayload) {
		this.shardKeyPayload = shardKeyPayload;
	}

	public int getShardID() {
		return shardID;
	}

	public final void setShardID(int shardID) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("(id: " + this.hashCode() + "): " + "setShardID(" + shardID + ")");
		}
		this.shardID = shardID;
	}
	
	public boolean enableDateNullFix() {
		return dateNullFixEnabled;
	}

	public E_DATASOURCE_TYPE getDataSource() {
		return datasource;
	}
	
	public void setFirstSQL(boolean isFirstSQL) {
		getHeraClient().setFirstSQL(isFirstSQL);
	}
}
