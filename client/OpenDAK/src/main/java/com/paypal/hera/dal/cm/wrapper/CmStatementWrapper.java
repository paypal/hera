package com.paypal.hera.dal.cm.wrapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 * Wraps JDBC Statement to allow custom processing of exceptions
 * Keeps track of child objects and closes them when this wrapper is closed
 * 
 */
class CmStatementWrapper extends CmBaseWrapper implements Statement
{
	protected final CmConnectionWrapper m_connection;
	private Statement m_statement;
	private ResultSet m_resultSet;
	
	private static final String UNSUPPORTED_OPERATION = "Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.";
	private static final String UNSUPPORTED_OPERATION_1_7 = "Not Implemented - Required for JDK 1.7 compliance.";

	CmStatementWrapper(CmConnectionWrapper connection, Statement statement)
	{
		super(connection);
		m_connection = connection;
		m_statement = statement;
	}

	SQLException cmProcessException(
		JdbcOperationType opType, SQLException e)
	{
		return m_connection.cmProcessException(m_statement, opType, e);
	}

	void cmCallStart(JdbcOperationType opType) throws SQLException
	{
		m_connection.cmCallStart(m_statement, opType);
		startUse(opType);
	}

	void cmCallEnd(JdbcOperationType opType, SQLException e) {
		endUse(opType);
		m_connection.cmCallEnd(m_statement, opType, e);
	}

	static Statement unwrap(Statement stmt) {
		if (!(stmt instanceof CmStatementWrapper)) {
			return stmt;
		}

		return ((CmStatementWrapper)stmt).m_statement;
	}

	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		synchronized (getLock()) {
			closeInternal();
		}
	}

	public void parentClosed() throws SQLException {
		if (isClosed()) {
			return;
		}

		// do not synchronize as connection's close may be invoked
		// from a different thread
		closeInternal();
	}

	private void closeInternal()
		throws SQLException
	{
		if (isClosed()) {
			return;
		}

		SQLException sqlexception = null;
		try {
			super.close();
		} catch (SQLException sqle) {
			sqlexception = sqle;
		}

		try {
			if (m_statement != null) {
				m_statement.close();
			}
		} catch (SQLException e) {
			if (sqlexception != null) {
				cmProcessException(JdbcOperationType.STMT_CLOSE, e);
			} else {
				sqlexception = cmProcessException(
					JdbcOperationType.STMT_CLOSE, e);
			}
		} finally {
			m_statement = null;
			eraseStmtPointers();
		}

		if (sqlexception != null) {
			throw sqlexception;
		}
	}

	protected void eraseStmtPointers() {
	}

	CmConnectionWrapper getConnectionWrapper()
		throws SQLException
	{
		checkOpened();
		return m_connection;
	}
	
	public Connection getConnection()
		throws SQLException
	{
		checkOpened();
		return m_connection;
	}

	public void setFetchDirection(int i)
		throws SQLException
	{
		checkOpened();
		m_statement.setFetchDirection(i);
	}

	public int getFetchDirection()
		throws SQLException
	{
		checkOpened();
		return m_statement.getFetchDirection();
	}

	public void setFetchSize(int i)
		throws SQLException
	{
		checkOpened();
		m_statement.setFetchSize(i);
	}

	public int getFetchSize()
		throws SQLException
	{
		checkOpened();
		return m_statement.getFetchSize();
	}

	public int getResultSetType()
		throws SQLException
	{
		checkOpened();
		return m_statement.getResultSetType();
	}

	public int getResultSetConcurrency()
		throws SQLException
	{
		checkOpened();
		return m_statement.getResultSetConcurrency();
	}

	public int getMaxFieldSize()
		throws SQLException
	{
		checkOpened();
		return m_statement.getMaxFieldSize();
	}

	public int getMaxRows()
		throws SQLException
	{
		checkOpened();
		return m_statement.getMaxRows();
	}

	public boolean getMoreResults()
		throws SQLException
	{
		checkOpened();
		return m_statement.getMoreResults();
	}

	public int getQueryTimeout()
		throws SQLException
	{
		checkOpened();
		return m_statement.getQueryTimeout();
	}

	public void cancel()
		throws SQLException
	{
		checkOpened();
		try {
			m_statement.cancel();
		} catch(SQLException e) {
			throw cmProcessException(
				JdbcOperationType.STMT_CANCEL, e);
		}
	}

	public void clearWarnings()
		throws SQLException
	{
		checkOpened();
		m_statement.clearWarnings();
	}

	public int getUpdateCount()
		throws SQLException
	{
		checkOpened();
		return m_statement.getUpdateCount();
	}

	public SQLWarning getWarnings()
		throws SQLException
	{
		checkOpened();
		return m_statement.getWarnings();
	}

	public void setCursorName(String s)
		throws SQLException
	{
		checkOpened();
		m_statement.setCursorName(s);
	}

	public void setEscapeProcessing(boolean value)
		throws SQLException
	{
		checkOpened();
		m_statement.setEscapeProcessing(value);
	}

	public void setMaxFieldSize(int size)
		throws SQLException
	{
		checkOpened();
		m_statement.setMaxFieldSize(size);
	}

	public void setMaxRows(int value)
		throws SQLException
	{
		checkOpened();
		m_statement.setMaxRows(value);
	}

	public void setQueryTimeout(int value)
		throws SQLException
	{
		checkOpened();
		m_statement.setQueryTimeout(value);
	}

	public ResultSet getResultSet()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.STMT_GET_RESULTSET);
				ResultSet resultset = m_statement.getResultSet();
				if (resultset == null) {
					m_resultSet = null;
				} else {
					m_resultSet = new CmResultSetWrapper(this, resultset);
				}
				return m_resultSet;
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.STMT_GET_RESULTSET, e);
			} finally {
				cmCallEnd(JdbcOperationType.STMT_GET_RESULTSET,
					sqlexception);
			}
		}
	}

	public boolean execute(String sql)
		throws SQLException
	{
		return execute(sql, false,
			JdbcOperationType.STMT_EXEC);
	}

	protected boolean execute(String sql,
		boolean isPrepared, JdbcOperationType opType) throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			boolean result = false;
			try {
				cmCallStart(opType);
				if (isPrepared) {
					result = ((PreparedStatement)m_statement).execute();
				} else {
					result = m_statement.execute(sql);
				}
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
			return result;
		}
	}

	public ResultSet executeQuery(String sql)
		throws SQLException
	{
		return executeQuery(sql, false,
			JdbcOperationType.STMT_EXEC_QUERY);
	}

	protected ResultSet executeQuery(String sql,
		boolean isPrepared, JdbcOperationType opType) throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			ResultSet resultset = null;
			try {
				cmCallStart(opType);
				if (m_resultSet != null) {
					try {
						m_resultSet.close();
					} finally {
						m_resultSet = null;
					}
				}
				if (isPrepared) {
					resultset = ((PreparedStatement)m_statement).executeQuery();
				} else {
					resultset = m_statement.executeQuery(sql);
				}
				if (resultset != null)
				{
					m_resultSet = new CmResultSetWrapper(this, resultset);
					resultset = m_resultSet;
				}
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
			return resultset;
		}
	}

	public int executeUpdate(String sql)
		throws SQLException
	{
		return executeUpdate(sql, false,
			JdbcOperationType.STMT_EXEC_UPDATE);
	}

	protected int executeUpdate(String sql,
		boolean isPrepared, JdbcOperationType opType)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			int result = -1;
			try {
				cmCallStart(opType);
				if (isPrepared) {
					result = ((PreparedStatement)m_statement).executeUpdate();
				} else {
					result = m_statement.executeUpdate(sql);
				}
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
			return result;
		}
	}

	public void addBatch(String sql)
		throws SQLException
	{
		checkOpened();
		m_statement.addBatch(sql);
	}

	public void clearBatch()
		throws SQLException
	{
		checkOpened();
		m_statement.clearBatch();
	}

	public int[] executeBatch()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			int result[] = null;
			try
			{
				cmCallStart(JdbcOperationType.PREP_STMT_EXEC_BATCH);
				result = m_statement.executeBatch();
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(
					JdbcOperationType.PREP_STMT_EXEC_BATCH,
					sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_EXEC_BATCH,
						sqlexception);
					throw sqlexception;
				}
			}
			return result;
		}
	}

	/**
	 * Returns a ResultSet object which  contains
	 * the auto generated keys
	 * @return ResultSet
	 * @throws SQLException
	 */
	public ResultSet getGeneratedKeys() throws SQLException {
		checkOpened();
		return m_statement.getGeneratedKeys();
	}

	// The methods below were added to make this impl. compatible with
	// jdbc  3.0 (Added as part of ESDBP348 by jsujela@ebay.com

	// Util method to throw exceptions for unimplemented methods
	private void throwUnsupportedMethodException(){
		throw new UnsupportedOperationException(
				"One of the new JDBC 3.0 methods have been invoked. " +
				"These methods are not supported in this version");
	}

	
	public boolean getMoreResults(int current) throws SQLException {
		throwUnsupportedMethodException();
		return false;
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return 0;
	}

	public int executeUpdate(String sql, int[] columnIndexes)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return 0;
	}

	public int executeUpdate(String sql, String[] columnNames)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return 0;
	}

	public boolean execute(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return false;
	}

	public boolean execute(String sql, int[] columnIndexes)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return false;
	}

	public boolean execute(String sql, String[] columnNames)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return false;
	}

	public int getResultSetHoldability() throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public boolean isPoolable() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}
	
	public void closeOnCompletion() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public boolean isCloseOnCompletion() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
}
