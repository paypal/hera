/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.paypal.hera.jdbc;

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

/**
 * This class implements all of the methods of the ResultSet interface, by
 * throwing an exception for each one. Subclasses (such as {@link JdbcResultSet}
 * ) can override those methods that it want to implement.
 */
public class ResultSetAdapter implements ResultSet {
	@Override
	public boolean absolute(int row) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void beforeFirst() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void afterLast() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void close() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean first() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getFetchSize() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public int getType() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isFirst() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isLast() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean last() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean next() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean previous() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader x, int length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader x, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(int columnIndex, NClob nclob) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(String columnLabel, NClob nclob)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNString(int columnIndex, String nstring)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNString(String columnLabel, String nstring)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scale)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scale)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("operation not implemented");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("operation not implemented");
	}
}
