package com.paypal.hera.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import com.paypal.hera.client.HeraClient;
import com.paypal.hera.conf.HeraClientConfigHolder;
import com.paypal.hera.ex.HeraClientException;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraRuntimeException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.util.HeraJdbcConverter;

public class HeraResultSet implements ResultSet {

	private boolean closed;
	private HeraConnection connection; 
	private HeraClient hera; 
	private HeraStatement stmt;
	private ResultSetMetaData metaData;
	private int currRowIdx;
	private boolean isFetchDone;
	private ArrayList<ArrayList<byte[]> > data;
	private int fetchSize;
	
	private HashMap<String, Integer> columnIndexes = null;
	private boolean paramWasNull;
	private HeraJdbcConverter converter; 
	
	public HeraResultSet(HeraConnection _connection, HeraStatement heraStatement, HeraClient _hera, int _fetchSize) throws SQLException {
		connection = _connection;
		closed = false;
		stmt = heraStatement;
		metaData = null;
		hera = _hera;
		fetchSize = _fetchSize;
		isFetchDone = false;
		paramWasNull = false;
		converter = ((HeraConnection)(heraStatement.getConnection())).getConverter();
		columnIndexes = heraStatement.getColumnIndexes();
		fetchNext(false);
	}
	/*** an "empty" resultset to please/fool hibernate***/
	public  HeraResultSet(){
		isFetchDone = true;
		data = new ArrayList<ArrayList<byte[]>>();
		currRowIdx = 0;
	}
	void fetchAllData() throws HeraExceptionBase {
		while (!isFetchDone) {
			fetchNext(true/*append new fetch data*/);
		}
	}
	
	private void helperClose(boolean nice) throws HeraExceptionBase {
		if (closed) {
			return;
		}
		closed = true;
		if (nice)
			fetchAllData();
		if (data != null) {
			data.clear();
			data = null;
		}
	}
	
	void hardClose() throws HeraExceptionBase {
		helperClose(false);
	}
	
	public void close() throws HeraExceptionBase {
		helperClose(true);
	}

	private void checkOpened() throws HeraRuntimeException {
		if (closed)
			throw new HeraRuntimeException("HeraResultSet is closed");
	}

	/***********************************************
	 *** GENERAL CALLS                           ***
	 ***********************************************/

	public Statement getStatement() throws SQLException {
		checkOpened();
		return stmt;
	}

	public void setFetchSize(int rows) throws SQLException {
		checkOpened();
		throw new HeraSQLException("Fetch size can be changed on statement only");
	}

	public int getFetchSize() throws SQLException {
		checkOpened();
		return stmt.getFetchSize();
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		checkOpened();
		if ((connection.supportResultSetMetaData()) && (metaData == null)) {
			HeraResultSetMetaData herametaData = new HeraResultSetMetaData(stmt.getColumnMeta());	
			herametaData.setPayloadSizeInBytes(getPayloadSizeInByteForCurrentFetch());
			metaData = herametaData;
		}
		return metaData;
	}
	
	private int getPayloadSizeInByteForCurrentFetch() throws SQLException {
		if (data==null){
			return 0;
		}
		int size = 0;
		for ( ArrayList<byte[]> row : data ){	
			for ( byte[] col : row ){
				size += col.length;
			}
		}
		return size;
	}

	public int findColumn(String columnName) throws SQLException {
		checkOpened();
		if (columnIndexes == null)
			throw new HeraClientException("Cannot get column name, please check the config for properties " + HeraClientConfigHolder.SUPPORT_COLUMN_NAMES_PROPERTY +
					" or " + HeraClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY);
		return columnIndexes.get(columnName.toUpperCase());
	}
	
	public SQLWarning getWarnings() throws SQLException {
		checkOpened();
		return null;
	}

	public void clearWarnings() throws SQLException {
		checkOpened();
	}

	public void setFetchDirection(int direction) throws SQLException {
		if (direction != FETCH_FORWARD) {
			throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
		} else {
			checkOpened();
		}
	}

	public int getFetchDirection() throws SQLException {
		checkOpened();
		return FETCH_FORWARD;
	}

	public int getType() throws SQLException {
		checkOpened();
		return TYPE_FORWARD_ONLY;
	}

	public int getConcurrency() throws SQLException {
		checkOpened();
		return CONCUR_READ_ONLY;
	}

	public void refreshRow() throws SQLException {
		checkOpened();
		// do nothing, assume data is fresh enough
	}

	public boolean rowUpdated() throws SQLException {
		checkOpened();
		return false;
	}

	public boolean rowInserted() throws SQLException {
		checkOpened();
		return false;
	}

	public boolean rowDeleted() throws SQLException {
		checkOpened();
		return false;
	}

	/***********************************************
	 *** RESULTSET NAVIGATION                    ***
	 ***********************************************/

	public boolean next() throws SQLException {
		checkOpened();
		return moveNext();
	}

	private boolean moveNext() throws HeraExceptionBase {
		if (currRowIdx < (data.size() - 1)) {
			currRowIdx++;
			return true;
		} else {
			// JDBC4.0 spec: it is impl specific whether to return false or throw Ex if there is no current row already
			// (i.e. call next() return false, then call next() one more);
			// for simplicity we just return false
			if (isFetchDone)
				return false;
			fetchNext(false);
			return moveNext();
		}
	}

	private void fetchNext(boolean append) throws HeraExceptionBase {
		try {
			ArrayList<ArrayList<byte[]>> new_data = hera.fetch(fetchSize);
			if (append)
				data.addAll(new_data);
			else {
				data = new_data;
				currRowIdx = -1;
			}
			if ((fetchSize == 0) || (new_data.size() < fetchSize))
				isFetchDone = true;
		} catch(HeraIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(HeraTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}
	}

	// spec jdbc4.0: getRow(), isFirst() ... are optional for forward_only
	public int getRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only"); // always throw
	}

	public boolean isBeforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only"); // always throw
	}

	public boolean isAfterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only"); // always throw
	}

	public boolean isFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only"); // always throw
	}

	public boolean isLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only"); // always throw
	}

	public void beforeFirst() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
	}

	public void afterLast() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
	}

	public boolean first() throws SQLException {
		return absolute(1);
	}

	public boolean last() throws SQLException {
		return absolute(-1);
	}

	public boolean absolute(int row) throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
	}

	public boolean relative(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
	}

	public boolean previous() throws SQLException {
		throw new SQLFeatureNotSupportedException("Hera resultset is forward-only");
	}

	/***********************************************
	 *** GETTING ROW DATA                        ***
	 ***********************************************/

	// this is not useful since we can't distinguish between empty string and NULL anyway 
	public boolean wasNull() throws SQLException {
		checkOpened();
		return paramWasNull;
	}

	public Object getObject(int columnIndex) throws SQLException {
		ResultSetMetaData meta = getMetaData();
		int type = meta.getColumnType(columnIndex);
		switch (type) {
		case Types.VARCHAR: {
			String val = getString(columnIndex);
			if (wasNull())
				return null;
			return val;
		}
		case Types.INTEGER: {
			int val = getInt(columnIndex);
			if (wasNull())
				return null;
			return val;
		}
		case Types.NUMERIC: {
			long val = getLong(columnIndex);
			if (wasNull())
				return null;
			return val;
		}
		default:
			throw new SQLFeatureNotSupportedException();
		}
	}

	public Object getObject(String columnName) throws SQLException {
		checkOpened();
		return getObject(findColumn(columnName));
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public int getInt(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return 0;
		return HeraJdbcConverter.hera2int(value);
	}
	
	public int getInt(String columnName) throws SQLException {
		checkOpened();
		return getInt(findColumn(columnName));
	}

	public String getString(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null; // TODO: should we return "" ?
		return HeraJdbcConverter.hera2String(value);
	}

	public String getString(String columnName) throws SQLException {
		checkOpened();
		return getString(findColumn(columnName));
	}

	public long getLong(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return 0;
		return HeraJdbcConverter.hera2long(value);
	}

	public long getLong(String columnName) throws SQLException {
		checkOpened();
		return getLong(findColumn(columnName));
	}

	public float getFloat(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return 0;
		return HeraJdbcConverter.hera2float(value);
	}

	public float getFloat(String columnName) throws SQLException {
		checkOpened();
		return getFloat(findColumn(columnName));
	}

	public double getDouble(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return 0;
		return HeraJdbcConverter.hera2double(value);
	}

	public double getDouble(String columnName) throws SQLException {
		checkOpened();
		return getDouble(findColumn(columnName));
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		return value;
	}

	public byte[] getBytes(String columnName) throws SQLException {
		checkOpened();
		return getBytes(findColumn(columnName));
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		try {
			return converter.hera2timestamp(value);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Timestamp", e);
		}
	}

	// per https://dev.paypal.com/wiki/Voila/DOAndDAOGuidelines 
	// Date/Time/Timestamp are discuraged by data arch, use number instead to store Unix time 
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return getTimestamp(findColumn(columnName));
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal)
		throws SQLException
	{
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		try {
			return converter.hera2timestamp(value, cal);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Timestamp", e);
		}
	}

	public Timestamp getTimestamp(String columnName, Calendar cal)
		throws SQLException
	{
		return getTimestamp(findColumn(columnName), cal);
	}

	public Time getTime(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		try {
			return converter.hera2time(value);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Time", e);
		}
	}

	public Time getTime(String columnName) throws SQLException {
		return getTime(findColumn(columnName));
	}

	public Time getTime(int columnIndex, Calendar cal)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public Time getTime(String columnName, Calendar cal)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		try {
			return converter.hera2date(value);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Date", e);
		}
	}

	public Date getDate(String columnName) throws SQLException {
		return getDate(findColumn(columnName));
	}

	public Date getDate(int columnIndex, Calendar cal)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public Date getDate(String columnName, Calendar cal)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (!paramWasNull) {
			if (value[0] == (byte)'1')
				return true;
		}
		return false;
	}

	public boolean getBoolean(String columnName) throws SQLException {
		return getBoolean(findColumn(columnName));
	}

	public byte getByte(int columnIndex) throws SQLException {
		return (byte)getShort(columnIndex);
	}

	public byte getByte(String columnName) throws SQLException {
		return (byte)getShort(columnName);
	}

	public short getShort(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return 0;
		return HeraJdbcConverter.hera2short(value);
	}

	public short getShort(String columnName) throws SQLException {
		checkOpened();
		return getShort(findColumn(columnName));
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		return HeraJdbcConverter.hera2Blob(value);
	}

	public Blob getBlob(String columnName) throws SQLException {
		checkOpened();
		return getBlob(findColumn(columnName));
	}

	public Clob getClob(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		return HeraJdbcConverter.hera2Clob(value);
	}

	public Clob getClob(String columnName) throws SQLException {
		checkOpened();
		return getClob(findColumn(columnName));
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("AsciiStream" + " result value type is not supported");
	}

	public InputStream getAsciiStream(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException("AsciiStream" + " result value type is not supported");
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		return new ByteArrayInputStream(value);
	}

	public InputStream getBinaryStream(String columnName) throws SQLException {
		return getBinaryStream(findColumn(columnName));
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		return new StringReader(HeraJdbcConverter.hera2String(value));
	}

	public Reader getCharacterStream(String columnName) throws SQLException {
		return getCharacterStream(findColumn(columnName));
	}

	/**
	 * @deprecated
	 */
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("UnicodeStream" + " result value type is not supported");
	}

	/**
	 * @deprecated
	 */
	public InputStream getUnicodeStream(String columnName)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("UnicodeStream" + " result value type is not supported");
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkOpened();
		byte[] value = data.get(currRowIdx).get(columnIndex - 1);
		paramWasNull = (value.length == 0);
		if (paramWasNull)
			return null;
		return HeraJdbcConverter.hera2BigDecimal(value);
	}

	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		checkOpened();
		return getBigDecimal(findColumn(columnName));
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int columnIndex, int scale)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("BigDecimal with scale is deprecated" + " result value type is not supported");
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(String columnName, int scale)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("BigDecimal with scale is deprecated" + " result value type is not supported");
	}

	public Ref getRef(int i) throws SQLException {
		throw new SQLFeatureNotSupportedException("Ref" + " result value type is not supported");
	}

	public Ref getRef(String colName) throws SQLException {
		throw new SQLFeatureNotSupportedException("Ref" + " result value type is not supported");
	}

	public Array getArray(int i) throws SQLException {
		throw new SQLFeatureNotSupportedException("Array" + " result value type is not supported");
	}

	public Array getArray(String colName) throws SQLException {
		throw new SQLFeatureNotSupportedException("Array" + " result value type is not supported");
	}

	/***********************************************
	 *** UPDATING ROW DATA                       ***
	 ***********************************************/

	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void moveToCurrentRow() throws SQLException {
		checkOpened();
		// do nothing, we're on current row anyway
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateTimestamp(int columnIndex, Timestamp x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateObject(int columnIndex, Object x, int scale)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateNull(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBoolean(String columnName, boolean x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateByte(String columnName, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateShort(String columnName, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateInt(String columnName, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateLong(String columnName, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateFloat(String columnName, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateDouble(String columnName, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBigDecimal(String columnName, BigDecimal x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateString(String columnName, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBytes(String columnName, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateDate(String columnName, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateTime(String columnName, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateTimestamp(String columnName, Timestamp x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateAsciiStream(String columnName, InputStream x,
		int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateBinaryStream(String columnName, InputStream x,
		int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateCharacterStream(String columnName, Reader reader,
		int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateObject(String columnName, Object x, int scale)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateObject(String columnName, Object x)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException("Update operations are not supported on Hera resultset");
	}

	// JDBC3.0 COMPATIBILITY

	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public URL getURL(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRef(String columnName, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnName, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnName, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateArray(String columnName, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	// JDBC 4.0
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getNString(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
