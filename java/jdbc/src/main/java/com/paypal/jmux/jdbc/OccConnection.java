package com.paypal.jmux.jdbc;

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

import com.paypal.jmux.client.OccClient;
import com.paypal.jmux.client.OccClientFactory;
import com.paypal.jmux.conf.OCCClientConfigHolder;
import com.paypal.jmux.ex.OccConfigException;
import com.paypal.jmux.ex.OccExceptionBase;
import com.paypal.jmux.ex.OccIOException;
import com.paypal.jmux.ex.OccRuntimeException;
import com.paypal.jmux.ex.OccSQLException;
import com.paypal.jmux.util.OccJdbcConverter;
import com.paypal.jmux.util.OccStatementsCache;
import com.paypal.jmux.util.OccStatementsCachePool;

public class OccConnection implements Connection {
	static final Logger LOGGER = LoggerFactory.getLogger(OccConnection.class);
	
	private boolean isClosed;
	private OccClient occClient = null;
	private OccDatabaseMetadata metaData;
	private WeakHashMap<OccStatement, Integer> statements;
	private OccStatementsCache statementCache;
	private OccResultSet active_rs = null;
	private boolean supportRSMetaData = false;
	private boolean autoCommit;
	private OccJdbcConverter converter;
	private String server_name;
	private String url;
	private Integer minFetchSize;
	private boolean escapeEnabled;
	private boolean shardingEnabled;
	private boolean batchEnabled;
	private boolean paramNameBindingEnabled;
	private boolean isDBEncodingUTF8;
	private boolean dateNullFixEnabled;
	private byte[] shardKeyPayload;
	private int shardID;
	private static final String SERVER_LOGICAL_NAME = "host";
	private static final String CAL_LOGGING_OPTIONS = "calLogOption";

	public OccConnection(Properties _props, String _address, String _port, String _url) throws OccIOException, OccConfigException {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("(id: " + this.hashCode() + "): " + "Occ Connecting to " + _address + ":" + _port);
		}
		url = _url;
		try {
			OCCClientConfigHolder config = new OCCClientConfigHolder(_props);

			// the driver custom outh not needed since netclient is doing it
			// Authenticator auth = new Authenticator(config.getCustomAuth(), config.getUsername(), config.getEncryptedAuthKey());
			// auth.authenticate(responseStream, requestStream);
			
			occClient = OccClientFactory.createClient(config, _address, _port);
			occClient.setServerLogicalName(_props.getProperty(SERVER_LOGICAL_NAME));
			occClient.setCalLogOption(_props.getProperty(CAL_LOGGING_OPTIONS));
			statements = new WeakHashMap<OccStatement, Integer>();
			supportRSMetaData = config.getSupportRSMetadata();
			minFetchSize = config.getMinFetchSize();
			autoCommit = false; 
			converter = new OccJdbcConverter();
			statementCache = OccStatementsCachePool.getStatementsCache(_url);
			//occClient.sendClientInfo("init", "");
			escapeEnabled = config.enableEscape();
			shardingEnabled = config.enableSharding();
			batchEnabled = config.enableBatch();
			paramNameBindingEnabled = config.enableParamNameBinding();
			isDBEncodingUTF8 = config.isDBEncodingUTF8();
			dateNullFixEnabled = config.enableDateNullFix();
			setShardKeyPayload(null);
			setShardID(-1);
			
			// extract server name
			String server_info = occClient.sendClientInfo("init", "");
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

			occClient.setOccBoxName(server_name);
						
		} catch (OccConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new OccIOException(e);
		}
	}

	void unregister(OccStatement _st) {
		if (!isClosed)
			statements.remove(_st);
	}
	
	void checkOpened() throws OccRuntimeException {
		if (isClosed)
			throw new OccRuntimeException("Connection is closed");
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

	private void cleanup() throws OccExceptionBase {
		for (OccStatement st: statements.keySet()) {
			st.close();
		}
		statements.clear();
		statements = null;
		OccStatementsCachePool.releaseStatementsCache(url, statementCache);
	}
	
	public void hardClose() throws OccExceptionBase {
		close();
	}
	
	@Override
	public void close() throws OccExceptionBase {
		if (isClosed)
			return;
		
		isClosed = true;
		cleanup();
		try {
			occClient.close();
		} catch (Exception e) {
			LOGGER.error("Fail to release transport");
			throw new OccIOException(e);
		}
	}
	
	public Statement createStatement() throws SQLException {
		checkOpened();
		OccStatement st = new OccStatement(this);
		statements.put(st, 1);
		return st;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkOpened();
		OccPreparedStatement st = new OccPreparedStatement(this, sql);
		statements.put(st, 1);
		return st;
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		checkOpened();
		OccCallableStatement st = new OccCallableStatement(this, sql);
		statements.put(st, 1);
		return st;
	}

	private void checkStatementOptions(int resultSetType,
		int resultSetConcurrency) throws SQLException
	{
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new OccSQLException("Occ resultset must be forward-only");
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new OccSQLException("Occ resultset must be read-only");
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
			metaData = new OccDatabaseMetadata(this);
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
			throw new OccSQLException("Unsupported transactions isolation level " + level);
		}
	}

	public int getTransactionIsolation() throws SQLException {
		checkOpened();
		return TRANSACTION_READ_COMMITTED;
	}

	public void commit() throws SQLException {
		checkOpened();
		try {
			getOccClient().commit();
		} catch(OccIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public void rollback() throws SQLException {
		checkOpened();
		try {
			getOccClient().rollback();
		} catch(OccIOException ex) {
			hardClose();
			throw ex;
		}
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		checkOpened();
		if (readOnly) {
			throw new OccSQLException("Occ driver does not support " + "read-only transactions");
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
		throw new OccSQLException("Custom type maps are not supported by Occ");
	}

	// JDBC3.0 COMPATIBILITY

	private void notSupported() throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("Not supported on Occ connection");
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
		notSupported();
		return null;
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
		return new OccBlob();
	}

	public Clob createClob() throws SQLException {
		return new OccClob();
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
		throw new SQLFeatureNotSupportedException("OccConnection.getClientInfo is not implemented");
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.isValid is not implemented");
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
		throw new SQLFeatureNotSupportedException("OccConnection.isWrapperFor is not implemented");
		}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		notSupported();
		return null;
	}

	public OccClient getOccClient() {
		return occClient;
	}

	public void setActiveResultSet(OccResultSet active_rs) {
		this.active_rs = active_rs;
	}

	public OccResultSet getActiveResultSet() {
		return active_rs;
	}

	public boolean supportResultSetMetaData() {
		return supportRSMetaData;
	}

	public OccStatementsCache getStatementCache() {
		return statementCache;
	}

	public String getUrl() {
		return url;
	}

	public OccJdbcConverter getConverter() {
		return converter;
	}

	public Integer getMinFetchSize() {
		return minFetchSize;
	}

	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.setSchema is not implemented");
		}

	public String getSchema() throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.getSchema is not implemented");
		}

	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.abort is not implemented");
		}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.setNetworkTimeout is not implemented");
		}

	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("OccConnection.getNetworkTimeout is not implemented");
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
			return getOccClient().getNumShards();
		} catch(OccIOException ex) {
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
				getOccClient().setShard(getShardID());
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("(id: " + this.hashCode() + "): " + "setShardHint(" + _key + ")");
				}
				setShardKeyPayload((_key + "=" + _value).getBytes());
			}
		} catch(OccIOException ex) {
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
				getOccClient().setShard(-1);
			}
			setShardKeyPayload(null);
		} catch(OccIOException ex) {
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
}
