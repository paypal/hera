package com.paypal.hera.dal.cm.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 * Wraps JDBC PreparedStatement to allow custom processing of exceptions
 * 
 */
class CmPreparedStatementWrapper extends CmStatementWrapper
	implements PreparedStatement
{
	private PreparedStatement m_preparedStatement;
	private static final String UNSUPPORTED_OPERATION = "Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.";

	CmPreparedStatementWrapper(CmConnectionWrapper connectionproxy,
		PreparedStatement preparedstatement)
	{
		super(connectionproxy, preparedstatement);
		m_preparedStatement = preparedstatement;
	}

	PreparedStatement getRealPreparedStatement() {
		return m_preparedStatement;
	}

	protected void eraseStmtPointers() {
		super.eraseStmtPointers();
		m_preparedStatement = null;
	}

	public boolean execute()
		throws SQLException
	{
		return execute(null, true,
			JdbcOperationType.PREP_STMT_EXEC);
	}

	public ResultSet executeQuery()
		throws SQLException
	{
		return executeQuery(null, true,
			JdbcOperationType.PREP_STMT_EXEC_QUERY);
	}

	public int executeUpdate()
		throws SQLException
	{
		return executeUpdate(null, true,
			JdbcOperationType.PREP_STMT_EXEC_UPDATE);
	}

	public void clearParameters()
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.clearParameters();
	}

	public void addBatch()
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.addBatch();
	}

	public ResultSetMetaData getMetaData()
		throws SQLException
	{
		checkOpened();
		return m_preparedStatement.getMetaData();
	}

	public void setArray(int index, Array value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setArray(index, value);
	}

	public void setAsciiStream(int index, InputStream inputstream, int length)
		throws SQLException
	{
		// set stream is likely to cause a stream flush in DBIT,
		// which can cause a delay, so we need to make sure
		// that CM knows a long operation has started and
		// so CM will not select connection as orphaned
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.PREP_STMT_SET_PARAM);
				m_preparedStatement.setAsciiStream(index, inputstream, length);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
					throw sqlexception;
				}
			}
		}
	}

	public void setBigDecimal(int index, BigDecimal value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setBigDecimal(index, value);
	}

	public void setBinaryStream(int index, InputStream inputstream, int length)
		throws SQLException
	{
		// set stream is likely to cause a stream flush in DBIT,
		// which can cause a delay, so we need to make sure
		// that CM knows a long operation has started and
		// so CM will not select connection as orphaned
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.PREP_STMT_SET_PARAM);
				m_preparedStatement.setBinaryStream(index, inputstream, length);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
					throw sqlexception;
				}
			}
		}
	}

	public void setBlob(int index, Blob value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setBlob(index, value);
	}

	public void setBoolean(int index, boolean value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setBoolean(index, value);
	}

	public void setByte(int index, byte value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setByte(index, value);
	}

	public void setBytes(int index, byte value[])
		throws SQLException
	{
		// set bytes is likely to cause a stream flush in DBIT,
		// which can cause a delay, so we need to make sure
		// that CM knows a long operation has started and
		// so CM will not select connection as orphaned
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.PREP_STMT_SET_PARAM);
				// hook for the hack... Oracle
				m_connection.m_driverAdapter.setBytesParameter(
					m_preparedStatement, index, value);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
					throw sqlexception;
				}
			}
		}
	}

	public void setClob(int index, Clob value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setClob(index, value);
	}

	public void setCharacterStream(int index, Reader reader, int length)
		throws SQLException
	{
		// set stream is likely to cause a stream flush in DBIT,
		// which can cause a delay, so we need to make sure
		// that CM knows a long operation has started and
		// so CM will not select connection as orphaned
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.PREP_STMT_SET_PARAM);
				m_preparedStatement.setCharacterStream(index, reader, length);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
					throw sqlexception;
				}
			}
		}
	}

	public void setDate(int index, Date value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setDate(index, value);
	}

	public void setDate(int index, Date value, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setDate(index, value, calendar);
	}

	public void setDouble(int index, double value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setDouble(index, value);
	}

	public void setFloat(int index, float value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setFloat(index, value);
	}

	public void setInt(int index, int value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setInt(index, value);
	}

	public void setLong(int index, long value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setLong(index, value);
	}

	public void setNull(int index, int sqlType, String typeName)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setNull(index, sqlType, typeName);
	}

	public void setNull(int index, int sqlType)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setNull(index, sqlType);
	}

	public void setObject(int index, Object value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setObject(index, value);
	}

	public void setObject(int index, Object value, int sqlType)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setObject(index, value, sqlType);
	}

	public void setObject(int index, Object value, int sqlType, int scale)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setObject(index, value, sqlType, scale);
	}

	public void setRef(int index, Ref value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setRef(index, value);
	}

	public void setShort(int index, short value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setShort(index, value);
	}

	public void setString(int index, String value)
		throws SQLException
	{
		// set string is likely to cause a stream flush in DBIT,
		// which can cause a delay, so we need to make sure
		// that CM knows a long operation has started and
		// so CM will not select connection as orphaned
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.PREP_STMT_SET_PARAM);
				// hook for the hack... Oracle
				m_connection.m_driverAdapter.setStringParameter(
					m_preparedStatement, index, value,
					m_connection.isUtf8Db());
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(
						JdbcOperationType.PREP_STMT_SET_PARAM, sqlexception);
					throw sqlexception;
				}
			}
		}
	}

	public void setTime(int index, Time value)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setTime(index, value);
	}

	public void setTime(int index, Time value, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setTime(index, value, calendar);
	}

	public void setTimestamp(int index, Timestamp value)
		throws SQLException
	{
		checkOpened();
		
		// hook for the hack... Oracle
//		if (m_preparedStatement instanceof PoolablePreparedStatement) {
//			PoolablePreparedStatement m_poolablePreparedStatement = (PoolablePreparedStatement) m_preparedStatement;
//			m_poolablePreparedStatement.setTimestamp(index, value,
//					m_connection.m_driverAdapter);
//		} else {
			m_connection.m_driverAdapter.setTimestampParameter(m_preparedStatement, index, value);
//		}
	}

	public void setTimestamp(int index, Timestamp value, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		if (value == null) {
			m_preparedStatement.setNull(index, Types.TIMESTAMP);
		} else {
			m_preparedStatement.setTimestamp(index, value, calendar);
		}
	}

	/**
	 * @deprecated
	 */
	public void setUnicodeStream(int index, InputStream inputstream, int length)
		throws SQLException
	{
		checkOpened();
		m_preparedStatement.setUnicodeStream(index, inputstream, length);
	}

	// The methods below were added to make this impl. compatible with
	// jdbc  3.0 (Added as part of ESDBP348 by jsujela@ebay.com

	// Util method to throw exceptions for unimplemented methods
	private void throwUnsupportedMethodException(){
		throw new UnsupportedOperationException(
				"One of the new JDBC 3.0 methods have been invoked. " +
				"These methods are not supported in this version");
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
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
}
