package com.paypal.hera.jdbc;

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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import com.paypal.hera.constants.BindType;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.util.HeraJdbcConverter;
import com.paypal.hera.util.HeraStatementsCache;
import com.paypal.hera.util.Pair;

public class HeraCallableStatement extends HeraPreparedStatement implements CallableStatement {

	private SortedMap<Integer, byte[]> data;
	private Set<Integer> out_params;
	private boolean outParamWasNull;
	
	public HeraCallableStatement(HeraConnection heraConnection, String sql) {
		super(heraConnection, sql);
		data = new TreeMap<Integer, byte[]>();
		out_params = new TreeSet<Integer>();
		outParamWasNull = false;
	}

	protected int countOutParameter() { return out_params.size(); }

	private void bindOut() throws HeraExceptionBase {
		Map<String, String> paramPosToNameMap = stCache.getParamPosToNameMap();
		
		for (Integer i: out_params) {
			if ( connection.paramNameBindingEnabled() 
					&& paramPosToNameMap != null && !paramPosToNameMap.isEmpty()){
				String name = paramPosToNameMap.get (HeraStatementsCache.paramName(i));
				connection.getHeraClient().bindOut(name==null?HeraStatementsCache.paramName(i):name.trim());	
			} else {
				connection.getHeraClient().bindOut(HeraStatementsCache.paramName(i));
			}
			data.put(i, null);
		}
	}
	
	private void fetch_out_params() throws SQLException {
		try {
			ArrayList<byte[]> result = connection.getHeraClient().fetchOutBindVars(data.size()).get(0);
			Iterator<Integer> it = data.keySet().iterator();
			int index = 0;
			while (it.hasNext()) {
				data.put(it.next(), result.get(index));
				index++;
			}
		} catch(HeraIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(HeraTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}
	}
	
	/* not supported, use executeQuery() instead */
	public ResultSet executeQuery(String sql) throws SQLException {
		throw new HeraSQLException("Not supported");
	}

	public ResultSet executeQuery() throws SQLException {
		outParamWasNull = false;
		data.clear();
		helperInitExecute();
		if (out_params.size() > 0) {
			prepare();
			bindAndShardInfo();
			bindOut();
			helperExecuteQuery();
			fetch_out_params();
			return createRecordSet();
		} else {
			return super.executeQuery();
		}
	}

	/* not supported, use executeUpdate() instead */
	public int executeUpdate(String sql) throws SQLException {
		throw new HeraSQLException("Not supported");
	}
	
	public int executeUpdate() throws SQLException {
		data.clear();
		helperInitExecute();
		if (out_params.size() > 0) {
			prepare();
			bindAndShardInfo();
			bindOut();
			helperExecuteUpdate(false/*no commit*/);
			fetch_out_params();
			return 1;
		} else {
			return super.executeUpdate();
		}
	}
	
	public boolean execute() throws SQLException {
		data.clear();
		helperInitExecute();
		if (out_params.size() > 0) {
			prepare();
			bindAndShardInfo();
			bindOut();
			helperExecute(false/*no commit*/);
			fetch_out_params();
			return false;  
		} else {
			return super.execute();
		}
	}

	public void clearParameters() throws SQLException {
		super.clearParameters();
		out_params.clear();
	}

	public void registerOutParameter(int paramIndex,
		int sqlType, String typeName)
		throws SQLException
	{
		registerOutParameter(paramIndex, sqlType);
	}

	public void registerOutParameter(int paramIndex,
		int sqlType, int scale)
		throws SQLException
	{
		registerOutParameter(paramIndex, sqlType);
	}

	public void registerOutParameter(int paramIndex, int sqlType)
		throws SQLException
	{
		checkParamStart();
		out_params.add(new Integer(paramIndex));
	}

	public boolean wasNull() throws SQLException {
		checkOpened();
		return outParamWasNull;
	}

	public String getString(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2String(bytes);
	}

	public boolean getBoolean(int paramIndex) throws SQLException {
			throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBoolean is not implemented");
	}

	public byte getByte(int paramIndex) throws SQLException {
		checkOpened();
		return (byte)getShort(paramIndex);
	}

	public short getShort(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2short(bytes);
	}

	public int getInt(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2int(bytes);
	}

	public long getLong(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2long(bytes);
	}

	public float getFloat(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2float(bytes);
	}

	public double getDouble(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2double(bytes);
	}

	/**
	 * @deprecated
	 */
	public BigDecimal getBigDecimal(int paramIndex, int scale)
		throws SQLException
	{
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBigDecimal is not implemented");
		
	}

	public byte[] getBytes(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = converter.hex2Binary(data.get(paramIndex));
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return bytes;
	}

	public Date getDate(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		try {
			return connection.getConverter().hera2date(bytes);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Date", e);
		}
	}

	public Time getTime(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		try {
			return connection.getConverter().hera2time(bytes);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Time", e);
		}
	}

	public Timestamp getTimestamp(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		try {
			return connection.getConverter().hera2timestamp(bytes);
		} catch (ParseException e) {
			throw new HeraSQLException("Failed to parse Timestamp", e);
		}
	}

	public Object getObject(int paramIndex) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getObject is not implemented");
		
	}

	public BigDecimal getBigDecimal(int paramIndex) throws SQLException {
		checkOpened();
		byte[] bytes = data.get(paramIndex);
		outParamWasNull = ((bytes == null) || (bytes.length == 0));
		return HeraJdbcConverter.hera2BigDecimal(bytes);
	}

	public Object getObject(int paramIndex, Map<String, Class<?>> map) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getObject is not implemented");
		
	}

	public Ref getRef(int paramIndex) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getRef is not implemented");
		
	}

	public Blob getBlob(int paramIndex) throws SQLException {
		checkOpened();
		// hera doesn't support Blob for bind out
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBlob is not implemented");
		
	}

	public Clob getClob(int paramIndex) throws SQLException {
		checkOpened();
		// hera doesn't support Clob for bind out
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getClob is not implemented");
		
	}

	public Array getArray(int paramIndex) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getArray is not implemented");
		
	}

	public Date getDate(int paramIndex, Calendar cal) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getDate is not implemented");
		
	}

	public Time getTime(int paramIndex, Calendar cal) throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTime is not implemented");
		
	}

	public Timestamp getTimestamp(int paramIndex, Calendar cal)
		throws SQLException
	{
		checkOpened();
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTimestamp is not implemented");
		
	}


	public void registerOutParameter(String parameterName, int sqlType)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.registerOutParameter is not implemented");
	}

	public void registerOutParameter(String parameterName,
		int sqlType, int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.registerOutParameter is not implemented");
	}

	public void registerOutParameter(String parameterName,
		int sqlType, String typeName) throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.registerOutParameter is not implemented");
	}

	public URL getURL(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getURL is not implemented");
		
	}

	public String getString(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getString is not implemented");
		
	}

	public boolean getBoolean(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBoolean is not implemented");
		
	}

	public byte getByte(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getByte is not implemented");
		
	}

	public short getShort(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getShort is not implemented");
		
	}

	public int getInt(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getInt is not implemented");
		
	}

	public long getLong(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getLong is not implemented");
		
	}

	public float getFloat(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getFloat is not implemented");
		
	}

	public double getDouble(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getDouble is not implemented");
		
	}

	public byte[] getBytes(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBytes is not implemented");
		
	}

	public Date getDate(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getDate is not implemented");
		
	}

	public Time getTime(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTime is not implemented");
		
	}

	public Timestamp getTimestamp(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTimestamp is not implemented");
		
	}

	public Object getObject(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getObject is not implemented");
		
	}

	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBigDecimal is not implemented");
		
	}

	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getObject is not implemented");
		
	}

	public Ref getRef(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getRef is not implemented");
		
	}

	public Blob getBlob(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getBlob is not implemented");
		
	}

	public Clob getClob(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getClob is not implemented");
		
	}

	public Array getArray(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getArray is not implemented");
		
	}

	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getDate is not implemented");
		
	}

	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTime is not implemented");
		
	}

	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getTimestamp is not implemented");
		
	}

	public URL getURL(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getURL is not implemented");
		
	}

	public void setURL(String parameterName, URL val) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setURL is not implemented");
		}

	public void setNull(String parameterName, int sqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNull is not implemented");
		}

	public void setBoolean(String parameterName, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBoolean is not implemented");
		}

	public void setByte(String parameterName, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setByte is not implemented");
		}

	public void setShort(String parameterName, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setShort is not implemented");
		}

	public void setInt(String parameterName, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setInt is not implemented");
		}

	public void setLong(String parameterName, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setLong is not implemented");
		}

	public void setFloat(String parameterName, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setFloat is not implemented");
		}

	public void setDouble(String parameterName, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setDouble is not implemented");
		}

	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBigDecimal is not implemented");
		}

	public void setString(String parameterName, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setString is not implemented");
		}

	public void setBytes(String parameterName, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBytes is not implemented");
		}

	public void setDate(String parameterName, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setDate is not implemented");
		}

	public void setTime(String parameterName, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setTime is not implemented");
		}

	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setTimestamp is not implemented");
		}

	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setAsciiStream is not implemented");
		}

	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBinaryStream is not implemented");
		}

	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setObject is not implemented");
		}

	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setObject is not implemented");
		}

	public void setObject(String parameterName, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setObject is not implemented");
		}

	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setCharacterStream is not implemented");
		}

	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getMoreResults is not implemented");
		}

	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setDate is not implemented");
		}

	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setTimestamp is not implemented");
		}

	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNull is not implemented");
		}

	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getMoreResults is not implemented");
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getGeneratedKeys is not implemented");
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.executeUpdate is not implemented");
		
	}

	public boolean execute(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.execute is not implemented");
		
	}

	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getResultSetHoldability is not implemented");
		
	}

	// JDBC 4.0
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getCharacterStream is not implemented");
		
	}

	public Reader getCharacterStream(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getCharacterStream is not implemented");
		
	}

	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNCharacterStream is not implemented");
		
	}

	public Reader getNCharacterStream(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNCharacterStream is not implemented");
		
	}

	public NClob getNClob(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNClob is not implemented");
		
	}

	public NClob getNClob(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNClob is not implemented");
		
	}

	public String getNString(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNString is not implemented");
		
	}

	public String getNString(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getNString is not implemented");
		
	}

	public RowId getRowId(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getRowId is not implemented");
		
	}

	public RowId getRowId(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getRowId is not implemented");
	
	}

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getSQLXML is not implemented");
		
	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.getSQLXML is not implemented");
		
	}

	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setAsciiStream is not implemented");
		}

	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setAsciiStream is not implemented");
		}

	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBinaryStream is not implemented");
		}

	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBinaryStream is not implemented");
		}

	public void setBlob(String parameterName, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBlob is not implemented");
		}

	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBlob is not implemented");
		}

	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBlob is not implemented");
		}

	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setCharacterStream is not implemented");
		}

	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setCharacterStream is not implemented");
		}

	public void setClob(String parameterName, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setClob is not implemented");
		}

	public void setClob(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setClob is not implemented");
		}

	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setClob is not implemented");
		}

	public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNCharacterStream is not implemented");
		}

	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNCharacterStream is not implemented");
		}

	public void setNClob(String parameterName, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNClob(String parameterName, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNString(String parameterName, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNString is not implemented");
		}

	public void setRowId(String parameterName, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setRowId is not implemented");
		}

	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setSQLXML is not implemented");
		}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setAsciiStream is not implemented");
		}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setAsciiStream is not implemented");
		}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBinaryStream is not implemented");
		}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBinaryStream is not implemented");
		}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBlob is not implemented");
		}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setBlob is not implemented");
		}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setCharacterStream is not implemented");
		}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setCharacterStream is not implemented");
		}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setClob is not implemented");
		}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setClob is not implemented");
		}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNCharacterStream is not implemented");
		}

	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNCharacterStream is not implemented");
		}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNClob is not implemented");
		}

	public void setNString(int parameterIndex, String value) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setNString is not implemented");
		}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setRowId is not implemented");
		}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setSQLXML is not implemented");
	}

	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isClosed is not implemented");
		
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isPoolable is not implemented");
		
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.setPoolable is not implemented");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isPoolable is not implemented");
		
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isPoolable is not implemented");
		
	}

	public <T> T getObject(int parameterIndex, Class<T> type)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isPoolable is not implemented");
		
	}

	public <T> T getObject(String parameterName, Class<T> type)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraCallableStatement.isPoolable is not implemented");
		
	}
}
