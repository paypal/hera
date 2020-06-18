package com.paypal.hera.dal.cm.wrapper;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.paypal.hera.dal.cm.ExplicitStatementCache;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerFactory;
import com.paypal.hera.dal.cm.transaction.DalTransactionManagerImpl;
import com.paypal.hera.dal.jdbc.rt.AlterDbSessionCommand;
import com.paypal.hera.dal.jdbc.rt.JdbcDriverAdapter;
import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;
import com.paypal.integ.odak.OdakConnection;

/**
 * Wraps connection to allow custom processing of exceptions
 * Keeps track of child objects and closes them when this wrapper is closed
 * 
 */
public class CmConnectionWrapper extends CmBaseWrapper
	implements Connection, ExplicitStatementCache
{
	protected Connection m_connection;
	protected final JdbcDriverAdapter m_driverAdapter;

	private CmConnectionCallback m_callback;
	private Object m_lockObject;
	private boolean m_isUtf8DbKnown;
	private boolean m_isUtf8Db;
	private List m_dbSessionParameters;
	private boolean m_inTransaction;
	private boolean isForcedDestroy;
	
	private static final String UNSUPPORTED_OPERATION = "Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.";
	private static final String UNSUPPORTED_OPERATION_1_7 = "Not Implemented - Required for JDK 1.7 compliance.";
	
	public CmConnectionWrapper(Connection connection,
		CmConnectionCallback callback,
		JdbcDriverAdapter driverAdapter)
	{
		this(connection, callback, driverAdapter, null);
	}
	
	public CmConnectionWrapper(Connection connection,
		CmConnectionCallback callback,
		JdbcDriverAdapter driverAdapter,
		HashMap dbSessionParameters, 
		boolean inTransaction)
	{
		m_connection = connection;
		m_callback = callback;
		m_driverAdapter = driverAdapter;
		m_lockObject = new Object();
		
		if (m_connection == null || m_driverAdapter == null) {
			String error = m_connection == null ? "Connection can not be null " : "";
			error = error + m_driverAdapter == null ? " Driver Adapter can not be null" : "";
			error = error +	"in CmConnectionWrapper constructor";
			throw new NullPointerException(error);
		}

		if (dbSessionParameters != null) {
			m_dbSessionParameters = 
				m_driverAdapter.getDbSessionParameterList(dbSessionParameters);
		}
		m_inTransaction = inTransaction;
	}
	
	public CmConnectionWrapper(Connection connection,
			CmConnectionCallback callback,
			JdbcDriverAdapter driverAdapter,
			HashMap dbSessionParameters)
	{
		this(connection, callback, driverAdapter, dbSessionParameters, false);
	}
	

	public void setSessionParameters() throws SQLException {		
		if (m_dbSessionParameters != null) {
			for(int i = 0; i < m_dbSessionParameters.size(); i++) {
				AlterDbSessionCommand p = (AlterDbSessionCommand)
					m_dbSessionParameters.get(i);				
				p.setParameterValue(m_connection);
			}
		}
	}
	
	public static Connection unwrap(Connection conn) {
		if (!(conn instanceof CmConnectionWrapper)) {
			return conn;
		}

		return ((CmConnectionWrapper)conn).m_connection;
	}

	public static Statement unwrap(Statement stmt) {
		return CmStatementWrapper.unwrap(stmt);
	}

	public static ResultSet unwrap(ResultSet rs) {
		return CmResultSetWrapper.unwrap(rs);
	}

	boolean isUtf8Db() throws SQLException {
		if (!m_isUtf8DbKnown) {
			m_isUtf8Db = m_callback.cmIsUtf8Db();
			m_isUtf8DbKnown = true;
		}
		return m_isUtf8Db;
	}
	
	public boolean isInTransaction() {
		return m_inTransaction;
	}

	void cmCallStart(Statement stmt, JdbcOperationType opType)
		throws SQLException
	{
		m_callback.cmCallStart(m_connection, stmt, opType);
		startUse(opType);
	}

	void cmCallEnd(Statement stmt,
		JdbcOperationType opType, SQLException e)
	{
		endUse(opType);
		m_callback.cmCallEnd(m_connection, stmt, opType, e);
	}

	SQLException cmProcessException(Statement stmt,
		JdbcOperationType opType, SQLException e)
	{
		return m_callback.cmProcessException(m_connection, stmt, opType, e);
	}

	private void cmCallStart(JdbcOperationType opType)
		throws SQLException
	{
		cmCallStart(null, opType);
	}

	private void cmCallEnd(JdbcOperationType opType, SQLException e)
	{
		cmCallEnd(null, opType, e);
	}

	private SQLException cmProcessException(
		JdbcOperationType opType, SQLException e)
	{
		return cmProcessException(null, opType, e);
	}

	private void revertSessionParameter() throws SQLException {		
		if (m_dbSessionParameters != null &&
			m_dbSessionParameters.size() != 0) {
			// revert in reverse order.
			for(int i = m_dbSessionParameters.size() - 1; i >= 0; i--) {
				AlterDbSessionCommand p = (AlterDbSessionCommand) 
					m_dbSessionParameters.get(i);
					
				p.revertToOriginal(m_connection);		
			}
		}
	}
	
	public void forceDestroy() {
		try {
//			if (m_connection instanceof PoolableConnection) {
//				((PoolableConnection) m_connection).forceDestory();
//			} else
			if (m_connection instanceof OdakConnection) {
				((OdakConnection) m_connection).destroyConnection();
			}
		} catch (Throwable th) {
			// don't throw any error to user if conn destroy fails.
		}
		isForcedDestroy = true;

	}

	public void close()
		throws SQLException
	{
		if (m_connection == null || isClosed()) {
			return;
		}
		
		// don't close if it's used in a transaction, and still active
		if (isInTransaction()) {
			DalTransactionManagerImpl dalTransMgr = (DalTransactionManagerImpl)
				DalTransactionManagerFactory.getDalTransactionManager();
			if (dalTransMgr.hasConnection(this)) {
				return;
			}
		}
		
		SQLException sqlexception = null;
		try {
			revertSessionParameter();
		} catch (SQLException e) {
			sqlexception = e;
		}
		
		synchronized (getLock())
		{
			try {
				super.close();
			} catch (SQLException sqle) {
				sqlexception = sqle;
			} catch (RuntimeException e) {
				// @PMD:REVIEWED:CloseConnectionRule: by ichernyshev on 09/09/05
				// @PMD:REVIEWED:DbConnectionRule: by ichernyshev on 09/09/05
				Connection conn = m_connection;
				// @PMD:REVIEWED:DbConnectionRule: by ichernyshev on 09/09/05
				m_connection = null;
				if (conn != null) {
					m_callback.cmConnectionDestory(conn);
				}
				throw e;
			} catch (Error e) {
				// @PMD:REVIEWED:CloseConnectionRule: by ichernyshev on 09/09/05
				// @PMD:REVIEWED:DbConnectionRule: by ichernyshev on 09/09/05
				Connection conn = m_connection;
				// @PMD:REVIEWED:DbConnectionRule: by ichernyshev on 09/09/05
				m_connection = null;
				if (conn != null) {
					m_callback.cmConnectionDestory(conn);
				}
				throw e;
			}

			try {
				if (m_connection != null) {
					m_callback.cmConnectionClose(m_connection);
				}
			} catch (SQLException e) {
				if (sqlexception == null) {
					sqlexception = e;
				}
			} finally {
				// @PMD:REVIEWED:DbConnectionRule: by ichernyshev on 09/02/05
				m_connection = null;
			}

			if (sqlexception != null) {
				throw sqlexception;
			}
		}
	}

	public void setTypeMap(Map<String, Class<?>> value)
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.setTypeMap(value);
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void setAutoCommit(boolean value)
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.setAutoCommit(value);
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public boolean getAutoCommit()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.getAutoCommit();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public Map<String, Class<?>> getTypeMap()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.getTypeMap();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void setReadOnly(boolean value)
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.setReadOnly(value);
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public boolean isReadOnly()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.isReadOnly();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void setCatalog(String value)
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.setCatalog(value);
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public String getCatalog()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.getCatalog();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void setTransactionIsolation(int value)
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.setTransactionIsolation(value);
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public int getTransactionIsolation()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.getTransactionIsolation();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public SQLWarning getWarnings()
		throws SQLException
	{
		checkOpened();
		try {
			return m_connection.getWarnings();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void clearWarnings()
		throws SQLException
	{
		checkOpened();
		try {
			m_connection.clearWarnings();
		} catch (SQLException e) {
			throw cmProcessException(JdbcOperationType.CONN_MISC, e);
		}
	}

	public void commit()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_COMMIT);
				m_connection.commit();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.CONN_COMMIT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_COMMIT, sqlexception);
			}
		}
	}

	public void rollback()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_ROLLBACK);
				m_connection.rollback();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.CONN_ROLLBACK, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_ROLLBACK, sqlexception);
			}
		}
	}

	public DatabaseMetaData getMetaData()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_MISC);
				return m_connection.getMetaData();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.CONN_MISC, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_MISC, sqlexception);
			}
		}
	}

	public String nativeSQL(String sql)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_MISC);
				return m_connection.nativeSQL(sql);
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_MISC, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_MISC, sqlexception);
			}
		}
	}

	public Statement createStatement()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_CREATE_STMT);
				Statement stmt = m_connection.createStatement();
				CmStatementWrapper result =
					new CmStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_CREATE_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_CREATE_STMT,
					sqlexception);
			}
		}
	}

	public PreparedStatement prepareStatement(String sql)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_STMT);
				PreparedStatement stmt = m_connection.prepareStatement(sql);
				CmPreparedStatementWrapper result =
					new CmPreparedStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_STMT,
					sqlexception);
			}
		}
	}

	public CallableStatement prepareCall(String sql)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_CALL);
				CallableStatement stmt = m_connection.prepareCall(sql);
				CmCallableStatementWrapper result =
					new CmCallableStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_CALL, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_CALL,
					sqlexception);
			}
		}
	}

	public Statement createStatement(int resultSetType,
		int resultSetConcurrency) throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_CREATE_STMT);
				Statement stmt = m_connection.createStatement(
					resultSetType, resultSetConcurrency);
				CmStatementWrapper result =
					new CmStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_CREATE_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_CREATE_STMT,
					sqlexception);
			}
		}
	}

	public PreparedStatement prepareStatement(String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_STMT);
				PreparedStatement stmt = m_connection.prepareStatement(sql,
					resultSetType, resultSetConcurrency);
				CmPreparedStatementWrapper result =
					new CmPreparedStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_STMT,
					sqlexception);
			}
		}
	}

	/**
	 * Creates PreparedStatement from the Connection object
	 * and wraps it using CmPreparedStatementWrapper and returns it
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return PreparedStatement
	 * @throws SQLException
	 */
	public PreparedStatement prepareStatement(String sql,
			int autoGeneratedKeys)
			throws SQLException
	{
			checkOpened();
			synchronized (getLock())
			{
				SQLException sqlexception = null;
				try {
					cmCallStart(JdbcOperationType.CONN_PREP_STMT);
					PreparedStatement stmt = m_connection.prepareStatement(sql,
						autoGeneratedKeys);
					CmPreparedStatementWrapper result =
						new CmPreparedStatementWrapper(this, stmt);
					return result;
				} catch (SQLException e) {
					sqlexception = e;
					throw cmProcessException(
						JdbcOperationType.CONN_PREP_STMT, e);
				} finally {
					cmCallEnd(JdbcOperationType.CONN_PREP_STMT,
						sqlexception);
				}
			}
	}

	public CallableStatement prepareCall(String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_CALL);
				CallableStatement stmt = m_connection.prepareCall(sql,
					resultSetType, resultSetConcurrency);
				CmCallableStatementWrapper result =
					new CmCallableStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_CALL, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_CALL,
					sqlexception);
			}
		}
	}

	public PreparedStatement prepareStatement(Object key, String sql)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_STMT);
				PreparedStatement stmt = cache.prepareStatement(key, sql);
				CmPreparedStatementWrapper result =
					new CmPreparedStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_STMT,
					sqlexception);
			}
		}
	}

	public CallableStatement prepareCall(Object key, String sql)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_CALL);
				CallableStatement stmt = cache.prepareCall(key, sql);
				CmCallableStatementWrapper result =
					new CmCallableStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_CALL, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_CALL,
					sqlexception);
			}
		}
	}

	public PreparedStatement prepareStatement(Object key, String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_STMT);
				PreparedStatement stmt = cache.prepareStatement(key, sql,
					resultSetType, resultSetConcurrency);
				CmPreparedStatementWrapper result =
					new CmPreparedStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_STMT, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_STMT,
					sqlexception);
			}
		}
	}

	/**
	 * Gets the PreparedStatement object from ExplicitStatementCache
	 * and wraps it using CmPreparedStatementWrapper and returns it
	 * @param key
	 * @param sql
	 * @param autoGeneratedKeys
	 * @return PreparedStatement
	 * @throws SQLException
	 */
	public PreparedStatement prepareStatement(Object key, String sql, 
			int autoGeneratedKeys)
			throws SQLException {
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		synchronized (getLock()) {
			SQLException sqlException = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_STMT);
				PreparedStatement stmt = cache.prepareStatement(key, sql, 
						autoGeneratedKeys);
				CmPreparedStatementWrapper result =
					new CmPreparedStatementWrapper(this, stmt);
				return result;
			}catch(SQLException e) {
				sqlException = e;
				throw cmProcessException(JdbcOperationType.CONN_PREP_STMT, e);
			}finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_STMT, sqlException);
			}
		}
	}

	public CallableStatement prepareCall(Object key, String sql,
		int resultSetType, int resultSetConcurrency)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.CONN_PREP_CALL);
				CallableStatement stmt = cache.prepareCall(key, sql,
						resultSetType, resultSetConcurrency);
				CmCallableStatementWrapper result =
					new CmCallableStatementWrapper(this, stmt);
				return result;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.CONN_PREP_CALL, e);
			} finally {
				cmCallEnd(JdbcOperationType.CONN_PREP_CALL,
					sqlexception);
			}
		}
	}

	public PreparedStatement lookupPreparedStatement(Object key)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		PreparedStatement stmt = cache.lookupPreparedStatement(key);
		if (stmt == null) {
			return null;
		}

		return new CmPreparedStatementWrapper(this, stmt);
	}

	public CallableStatement lookupCallableStatement(Object key)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		CallableStatement stmt = cache.lookupCallableStatement(key);
		if (stmt == null) {
			return null;
		}

		return new CmCallableStatementWrapper(this, stmt);
	}

	public String getCachedStatementSQL(PreparedStatement stmt)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		PreparedStatement realStmt =
			((CmPreparedStatementWrapper)stmt).getRealPreparedStatement();
		return cache.getCachedStatementSQL(realStmt);
	}

	public void removeStatement(Object key)
		throws SQLException
	{
		checkOpened();
		ExplicitStatementCache cache = (ExplicitStatementCache)m_connection;
		cache.removeStatement(key);
	}

	protected Object getLock()
	{
		return m_lockObject;
	}

	// The methods below were added to make this impl. compatible with
	// jdbc  3.0 (Added as part of ESDBP348 by jsujela@ebay.com

	// Util method to throw exceptions for unimplemented methods
	private void throwUnsupportedMethodException(){
		throw new UnsupportedOperationException(
				"One of the new JDBC 3.0 methods have been invoked. " +
				"These methods are not supported in this version");
	}

	public void setHoldability(int holdability) throws SQLException {
		throwUnsupportedMethodException();
	}

	public int getHoldability() throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public Savepoint setSavepoint() throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throwUnsupportedMethodException();
	}

	public Statement createStatement(int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
		int resultSetConcurrency, int resultSetHoldability)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public PreparedStatement prepareStatement(String sql,
		int[] columnIndexes) throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public PreparedStatement prepareStatement(String sql,
		String[] columnNames) throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Blob createBlob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Clob createClob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public NClob createNClob() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public SQLXML createSQLXML() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Properties getClientInfo() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public String getClientInfo(String name) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}
	
	public void abort(Executor executor) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public int getNetworkTimeout() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public void setSchema(String schema) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public String getSchema() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}

	public boolean isForcedDestroy() {
		return isForcedDestroy;
	}
}
