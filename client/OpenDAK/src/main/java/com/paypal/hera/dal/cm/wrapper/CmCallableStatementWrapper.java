package com.paypal.hera.dal.cm.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Wraps JDBC CallableStatement to allow custom processing of exceptions
 * 
 */
class CmCallableStatementWrapper extends CmPreparedStatementWrapper
	implements CallableStatement
{
	private CallableStatement m_callableStatement;
	private static final String UNSUPPORTED_OPERATION = "Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.";
	private static final String UNSUPPORTED_OPERATION_1_7 = "Not Implemented - Required for JDK 1.7 compliance.";

	CmCallableStatementWrapper(CmConnectionWrapper connectionproxy,
		CallableStatement callablestatement)
	{
		super(connectionproxy, callablestatement);
		m_callableStatement = callablestatement;
	}

	protected void eraseStmtPointers() {
		super.eraseStmtPointers();
		m_callableStatement = null;
	}

	public void registerOutParameter(int index, int sqlType)
		throws SQLException
	{
		checkOpened();
		m_callableStatement.registerOutParameter(index, sqlType);
	}

	public void registerOutParameter(int index, int sqlType, String typeName)
		throws SQLException
	{
		checkOpened();
		m_callableStatement.registerOutParameter(index, sqlType, typeName);
	}

	public void registerOutParameter(int index, int sqlType, int scale)
		throws SQLException
	{
		checkOpened();
		m_callableStatement.registerOutParameter(index, sqlType, scale);
	}

	public boolean wasNull()
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.wasNull();
	}

	public Array getArray(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getArray(index);
	}

	public BigDecimal getBigDecimal(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getBigDecimal(index);
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int index, int scale)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getBigDecimal(index, scale);
	}

	public Blob getBlob(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getBlob(index);
	}

	public boolean getBoolean(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getBoolean(index);
	}

	public byte getByte(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getByte(index);
	}

	public byte[] getBytes(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getBytes(index);
	}

	public Clob getClob(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getClob(index);
	}

	public Date getDate(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getDate(index);
	}

	public Date getDate(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getDate(index, calendar);
	}

	public double getDouble(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getDouble(index);
	}

	public float getFloat(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getFloat(index);
	}

	public int getInt(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getInt(index);
	}

	public long getLong(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getLong(index);
	}

	public Object getObject(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getObject(index);
	}

	public Object getObject(int index, Map<String, Class<?>> map)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getObject(index, map);
	}

	public Ref getRef(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getRef(index);
	}

	public short getShort(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getShort(index);
	}

	public String getString(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getString(index);
	}

	public Time getTime(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getTime(index);
	}

	public Time getTime(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getTime(index, calendar);
	}

	public Timestamp getTimestamp(int index)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getTimestamp(index);
	}

	public Timestamp getTimestamp(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_callableStatement.getTimestamp(index, calendar);
	}

	
	// The methods below were added to make this impl. compatible with
	// jdbc  3.0 (Added as part of ESDBP348 by jsujela@ebay.com

	// Util method to throw exceptions for unimplemented methods
	private void throwUnsupportedMethodException(){
		throw new UnsupportedOperationException(
				"One of the new JDBC 3.0 methods have been invoked. " +
				"These methods are not supported in this version");
	}

	
	public void registerOutParameter(String parameterName, int sqlType)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void registerOutParameter(String parameterName, int sqlType,
		int scale) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void registerOutParameter(String parameterName, int sqlType,
		String typeName) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public URL getURL(int parameterIndex) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public void setURL(String parameterName, URL val) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setNull(String parameterName, int sqlType)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setBoolean(String parameterName, boolean x)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setByte(String parameterName, byte x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setShort(String parameterName, short x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setInt(String parameterName, int x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setLong(String parameterName, long x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setFloat(String parameterName, float x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setDouble(String parameterName, double x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setBigDecimal(String parameterName, BigDecimal x)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setString(String parameterName, String x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setBytes(String parameterName, byte[] x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setDate(String parameterName, Date x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setTime(String parameterName, Time x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setTimestamp(String parameterName, Timestamp x)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setAsciiStream(String parameterName, InputStream x,
		int length) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setBinaryStream(String parameterName, InputStream x,
		int length) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setObject(String parameterName, Object x, int targetSqlType,
		int scale) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setObject(String parameterName, Object x, int targetSqlType)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setObject(String parameterName, Object x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void setCharacterStream(String parameterName, Reader reader,
		int length) throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setDate(String parameterName, Date x, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setTime(String parameterName, Time x, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public void setNull(String parameterName, int sqlType, String typeName)
		throws SQLException
	{
		throwUnsupportedMethodException();
	}

	public String getString(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public boolean getBoolean(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return false;
	}

	public byte getByte(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public short getShort(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public int getInt(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public long getLong(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public float getFloat(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public double getDouble(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return 0;
	}

	public byte[] getBytes(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Date getDate(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Time getTime(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Timestamp getTimestamp(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Object getObject(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Ref getRef(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Blob getBlob(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Clob getClob(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Array getArray(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public Date getDate(String parameterName, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public Time getTime(String parameterName, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public Timestamp getTimestamp(String parameterName, Calendar cal)
		throws SQLException
	{
		throwUnsupportedMethodException();
		return null;
	}

	public URL getURL(String parameterName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public boolean getMoreResults(int current) throws SQLException {
		throwUnsupportedMethodException();
		return false;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throwUnsupportedMethodException();
		return null;
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

	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Reader getCharacterStream(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Reader getNCharacterStream(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public NClob getNClob(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public NClob getNClob(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public String getNString(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public String getNString(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public RowId getRowId(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public RowId getRowId(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBlob(String parameterName, Blob x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClob(String parameterName, Clob x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClob(String parameterName, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(String parameterName, NClob value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(String parameterName, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setNString(String parameterName, String value) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setRowId(String parameterName, RowId x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
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
	
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
}
