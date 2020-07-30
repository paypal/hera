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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 * Wraps JDBC ResultSet to allow custom processing of exceptions
 * 
 */
// @PMD:REVIEWED:ExcessiveClassLength: by ichernyshev on 09/02/05
class CmResultSetWrapper extends CmBaseWrapper implements ResultSet
{
	private CmStatementWrapper m_statement;	
	private ResultSet m_rs;
	
	private static final String UNSUPPORTED_OPERATION = "Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.";
	private static final String UNSUPPORTED_OPERATION_1_7 = "Not Implemented - Required for JDK 1.7 compliance.";

	CmResultSetWrapper(CmStatementWrapper statement, ResultSet resultset)
	{
		super(statement);
		m_statement = statement;
		m_rs = resultset;
	}

	static ResultSet unwrap(ResultSet rs) {
		if (!(rs instanceof CmResultSetWrapper)) {
			return rs;
		}

		return ((CmResultSetWrapper)rs).m_rs;
	}

	SQLException cmProcessException(
		JdbcOperationType opType, SQLException e)
	{
		return m_statement.cmProcessException(opType, e);
	}

	void cmCallStart(JdbcOperationType opType) throws SQLException
	{
		m_statement.cmCallStart(opType);
		startUse(opType);
	}

	void cmCallEnd(JdbcOperationType opType, SQLException e) {
		endUse(opType);
		m_statement.cmCallEnd(opType, e);
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
			if (m_rs != null) {
				m_rs.close();
			}
		} catch (SQLException e) {
			if (sqlexception != null) {
				cmProcessException(JdbcOperationType.RS_CLOSE, e);
			} else {
				sqlexception = cmProcessException(
					JdbcOperationType.RS_CLOSE, e);
			}
		} finally {
			m_rs = null;
		}

		if (sqlexception != null) {
			throw sqlexception;
		}
	}

	public Statement getStatement()
		throws SQLException
	{
		checkOpened();
		return m_statement;
	}


	public boolean next()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_NEXT);
				return m_rs.next();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_NEXT, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_NEXT, sqlexception);
			}
		}
	}

	public void afterLast()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				m_rs.afterLast();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public void beforeFirst()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				m_rs.beforeFirst();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public boolean first()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				return m_rs.first();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public boolean last()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				return m_rs.last();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
			
		}
	}

	public boolean absolute(int pos)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				return m_rs.absolute(pos);
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public boolean relative(int pos)
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				return m_rs.relative(pos);
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public boolean previous()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_POS_CHANGE);
				return m_rs.previous();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(
					JdbcOperationType.RS_POS_CHANGE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_POS_CHANGE, sqlexception);
			}
		}
	}

	public void insertRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.insertRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void updateRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.updateRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void deleteRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.deleteRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void refreshRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.refreshRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void cancelRowUpdates()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.cancelRowUpdates();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void moveToInsertRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.moveToInsertRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public void moveToCurrentRow()
		throws SQLException
	{
		checkOpened();
		synchronized (getLock())
		{
			SQLException sqlexception = null;
			try {
				cmCallStart(JdbcOperationType.RS_UPDATE);
				m_rs.moveToCurrentRow();
			} catch (SQLException e) {
				sqlexception = e;
				throw cmProcessException(JdbcOperationType.RS_UPDATE, e);
			} finally {
				cmCallEnd(JdbcOperationType.RS_UPDATE, sqlexception);
			}
		}
	}

	public SQLWarning getWarnings()
		throws SQLException
	{
		checkOpened();
		return m_rs.getWarnings();
	}

	public void clearWarnings()
		throws SQLException
	{
		checkOpened();
		m_rs.clearWarnings();
	}

	public String getCursorName()
		throws SQLException
	{
		checkOpened();
		return m_rs.getCursorName();
	}

	public ResultSetMetaData getMetaData()
		throws SQLException
	{
		checkOpened();
		return m_rs.getMetaData();
	}

	public int findColumn(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.findColumn(columnName);
	}

	public int getConcurrency()
		throws SQLException
	{
		checkOpened();
		return m_rs.getConcurrency();
	}

	public int getFetchDirection()
		throws SQLException
	{
		checkOpened();
		return m_rs.getFetchDirection();
	}

	public int getFetchSize()
		throws SQLException
	{
		checkOpened();
		return m_rs.getFetchSize();
	}

	public boolean wasNull()
		throws SQLException
	{
		checkOpened();
		return m_rs.wasNull();
	}

	public int getType()
		throws SQLException
	{
		checkOpened();
		return m_rs.getType();
	}

	public int getRow()
		throws SQLException
	{
		checkOpened();
		return m_rs.getRow();
	}

	public boolean isAfterLast()
		throws SQLException
	{
		checkOpened();
		return m_rs.isAfterLast();
	}

	public boolean isBeforeFirst()
		throws SQLException
	{
		checkOpened();
		return m_rs.isBeforeFirst();
	}

	public boolean isFirst()
		throws SQLException
	{
		checkOpened();
		return m_rs.isFirst();
	}

	public boolean isLast()
		throws SQLException
	{
		checkOpened();
		return m_rs.isLast();
	}

	public boolean rowDeleted()
		throws SQLException
	{
		checkOpened();
		return m_rs.rowDeleted();
	}

	public boolean rowInserted()
		throws SQLException
	{
		checkOpened();
		return m_rs.rowInserted();
	}

	public boolean rowUpdated()
		throws SQLException
	{
		checkOpened();
		return m_rs.rowUpdated();
	}

	public void setFetchDirection(int direction)
		throws SQLException
	{
		checkOpened();
		m_rs.setFetchDirection(direction);
	}

	public void setFetchSize(int size)
		throws SQLException
	{
		checkOpened();
		m_rs.setFetchSize(size);
	}

	public void updateAsciiStream(int index,
		InputStream inputstream, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateAsciiStream(index, inputstream, length);
	}

	public void updateAsciiStream(String columnName,
		InputStream inputstream, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateAsciiStream(columnName, inputstream, length);
	}

	public void updateBigDecimal(int index, BigDecimal value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBigDecimal(index, value);
	}

	public void updateBigDecimal(String columnName, BigDecimal value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBigDecimal(columnName, value);
	}

	public void updateBinaryStream(int index,
		InputStream inputstream, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBinaryStream(index, inputstream, length);
	}

	public void updateBinaryStream(String columnName,
		InputStream inputstream, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBinaryStream(columnName, inputstream, length);
	}

	public void updateBoolean(int index, boolean value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBoolean(index, value);
	}

	public void updateBoolean(String columnName, boolean value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateBoolean(columnName, value);
	}

	public void updateByte(int index, byte value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateByte(index, value);
	}

	public void updateByte(String columnName, byte value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateByte(columnName, value);
	}

	public void updateBytes(int index, byte value[])
		throws SQLException
	{
		checkOpened();
		m_rs.updateBytes(index, value);
	}

	public void updateBytes(String columnName, byte value[])
		throws SQLException
	{
		checkOpened();
		m_rs.updateBytes(columnName, value);
	}

	public void updateCharacterStream(int index, Reader reader, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateCharacterStream(index, reader, length);
	}

	public void updateCharacterStream(String columnName,
		Reader reader, int length)
		throws SQLException
	{
		checkOpened();
		m_rs.updateCharacterStream(columnName, reader, length);
	}

	public void updateDate(int index, Date value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateDate(index, value);
	}

	public void updateDate(String columnName, Date value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateDate(columnName, value);
	}

	public void updateDouble(int index, double value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateDouble(index, value);
	}

	public void updateDouble(String columnName, double value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateDouble(columnName, value);
	}

	public void updateFloat(int index, float value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateFloat(index, value);
	}

	public void updateFloat(String columnName, float value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateFloat(columnName, value);
	}

	public void updateInt(int index, int value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateInt(index, value);
	}

	public void updateInt(String columnName, int value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateInt(columnName, value);
	}

	public void updateLong(int index, long value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateLong(index, value);
	}

	public void updateLong(String columnName, long value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateLong(columnName, value);
	}

	public void updateNull(int index)
		throws SQLException
	{
		checkOpened();
		m_rs.updateNull(index);
	}

	public void updateNull(String columnName)
		throws SQLException
	{
		checkOpened();
		m_rs.updateNull(columnName);
	}

	public void updateObject(int index, Object value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateObject(index, value);
	}

	public void updateObject(int index, Object value, int scale)
		throws SQLException
	{
		checkOpened();
		m_rs.updateObject(index, value, scale);
	}

	public void updateObject(String columnName, Object value, int scale)
		throws SQLException
	{
		checkOpened();
		m_rs.updateObject(columnName, value, scale);
	}

	public void updateObject(String columnName, Object value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateObject(columnName, value);
	}

	public void updateShort(int index, short value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateShort(index, value);
	}

	public void updateShort(String columnName, short value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateShort(columnName, value);
	}

	public void updateString(int index, String value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateString(index, value);
	}

	public void updateString(String columnName, String value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateString(columnName, value);
	}

	public void updateTime(int index, Time value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateTime(index, value);
	}

	public void updateTime(String columnName, Time value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateTime(columnName, value);
	}

	public void updateTimestamp(int index, Timestamp value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateTimestamp(index, value);
	}

	public void updateTimestamp(String columnName, Timestamp value)
		throws SQLException
	{
		checkOpened();
		m_rs.updateTimestamp(columnName, value);
	}

	public Array getArray(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getArray(index);
	}

	public Array getArray(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getArray(columnName);
	}

	public InputStream getAsciiStream(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getAsciiStream(index);
	}

	public InputStream getAsciiStream(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getAsciiStream(columnName);
	}

	public BigDecimal getBigDecimal(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBigDecimal(index);
	}

	public BigDecimal getBigDecimal(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBigDecimal(columnName);
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int index, int scale)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBigDecimal(index, scale);
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(String columnName, int scale)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBigDecimal(columnName, scale);
	}

	public InputStream getBinaryStream(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBinaryStream(index);
	}

	public InputStream getBinaryStream(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBinaryStream(columnName);
	}

	public Blob getBlob(int index)
		throws SQLException
	{
		checkOpened();
		Blob realBlob = m_rs.getBlob(index);
		return (realBlob == null ? null : 
			new CmBlobWrapper(m_statement.getConnectionWrapper(), realBlob));
	}

	public Blob getBlob(String columnName)
		throws SQLException
	{
		checkOpened();
		Blob realBlob = m_rs.getBlob(columnName);
		return (realBlob == null ? null : 
			new CmBlobWrapper(m_statement.getConnectionWrapper(), realBlob));
	}

	public boolean getBoolean(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBoolean(index);
	}

	public boolean getBoolean(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBoolean(columnName);
	}

	public byte getByte(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getByte(index);
	}

	public byte getByte(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getByte(columnName);
	}

	public byte[] getBytes(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBytes(index);
	}

	public byte[] getBytes(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getBytes(columnName);
	}

	public Clob getClob(int index)
		throws SQLException
	{
		checkOpened();
		Clob realClob = m_rs.getClob(index);
		return (realClob == null ? null : 
			new CmClobWrapper(m_statement.getConnectionWrapper(), realClob));
	}

	public Clob getClob(String columnName)
		throws SQLException
	{
		checkOpened();
		Clob realClob = m_rs.getClob(columnName);
		return (realClob == null ? null : 
			new CmClobWrapper(m_statement.getConnectionWrapper(), realClob));
	}

	public Reader getCharacterStream(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getCharacterStream(index);
	}

	public Reader getCharacterStream(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getCharacterStream(columnName);
	}

	public Date getDate(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDate(index);
	}

	public Date getDate(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDate(columnName);
	}

	public Date getDate(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDate(index, calendar);
	}

	public Date getDate(String columnName, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDate(columnName, calendar);
	}

	public double getDouble(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDouble(index);
	}

	public double getDouble(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getDouble(columnName);
	}

	public float getFloat(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getFloat(index);
	}

	public float getFloat(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getFloat(columnName);
	}

	public int getInt(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getInt(index);
	}

	public int getInt(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getInt(columnName);
	}

	public long getLong(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getLong(index);
	}

	public long getLong(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getLong(columnName);
	}

	public Object getObject(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getObject(index);
	}

	public Object getObject(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getObject(columnName);
	}

	public Object getObject(int index, Map<String, Class<?>> map)
		throws SQLException
	{
		checkOpened();
		return m_rs.getObject(index, map);
	}

	public Object getObject(String columnName, Map<String, Class<?>> map)
		throws SQLException
	{
		checkOpened();
		return m_rs.getObject(columnName, map);
	}

	public Ref getRef(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getRef(index);
	}

	public Ref getRef(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getRef(columnName);
	}

	public short getShort(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getShort(index);
	}

	public short getShort(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getShort(columnName);
	}

	public String getString(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getString(index);
	}

	public String getString(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getString(columnName);
	}

	public Time getTime(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTime(index);
	}

	public Time getTime(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTime(columnName);
	}

	public Time getTime(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTime(index, calendar);
	}

	public Time getTime(String columnName, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTime(columnName, calendar);
	}

	public Timestamp getTimestamp(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTimestamp(index);
	}

	public Timestamp getTimestamp(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTimestamp(columnName);
	}

	public Timestamp getTimestamp(int index, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTimestamp(index, calendar);
	}

	public Timestamp getTimestamp(String columnName, Calendar calendar)
		throws SQLException
	{
		checkOpened();
		return m_rs.getTimestamp(columnName, calendar);
	}

	/**
	 * @deprecated
	 */
	public InputStream getUnicodeStream(int index)
		throws SQLException
	{
		checkOpened();
		return m_rs.getUnicodeStream(index);
	}

	/**
	 * @deprecated
	 */
	public InputStream getUnicodeStream(String columnName)
		throws SQLException
	{
		checkOpened();
		return m_rs.getUnicodeStream(columnName);
	}

	// The following methods were added to make the imple JDBC 3.0 compliant
	
	private void throwUnsupportedMethodException(){
		throw new UnsupportedOperationException(
				"One of the new JDBC 3.0 methods have been invoked. " +
				"These methods are not supported in this version");
	}

	public URL getURL(int columnIndex) throws SQLException {

		throwUnsupportedMethodException();
		return null;
	}

	public URL getURL(String columnName) throws SQLException {
		throwUnsupportedMethodException();
		return null;
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateRef(String columnName, Ref x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateBlob(String columnName, Blob x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateClob(String columnName, Clob x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public void updateArray(String columnName, Array x) throws SQLException {
		throwUnsupportedMethodException();
	}

	public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public String getNString(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public String getNString(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_1_7);
	}
	
// @PMD:REVIEWED:ExcessivePublicCountRule: by ichernyshev on 09/02/05
}
