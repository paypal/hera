package com.paypal.integ.odak.stmts;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
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
import java.util.Calendar;

/**
 * 
 * NOT IN USE currently
 * 
 * Do we need Statement and CallableStatement (extends PreparedStatement).
 * 
 * Why use Statement directly with OCC use cases?
 * 
 *
 */
public class OdakPreparedStatement extends OdakStatement implements PreparedStatement {
	private PreparedStatement pStatement;
	private Connection conn;

	public OdakPreparedStatement(Connection conn, PreparedStatement preparedStatement) {
		super(conn, preparedStatement);
		this.conn = conn;
		this.pStatement = preparedStatement;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return pStatement.executeQuery();
	}

	@Override
	public int executeUpdate() throws SQLException {
		return pStatement.executeUpdate();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		pStatement.setNull(parameterIndex, sqlType);

	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		pStatement.setBoolean(parameterIndex, x);

	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		pStatement.setByte(parameterIndex, x);

	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		pStatement.setShort(parameterIndex, x);

	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		pStatement.setInt(parameterIndex, x);

	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		pStatement.setLong(parameterIndex, x);

	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		pStatement.setFloat(parameterIndex, x);

	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		pStatement.setDouble(parameterIndex, x);

	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		pStatement.setBigDecimal(parameterIndex, x);

	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		pStatement.setString(parameterIndex, x);

	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		pStatement.setBytes(parameterIndex, x);

	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		pStatement.setDate(parameterIndex, x);

	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		pStatement.setTime(parameterIndex, x);

	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		pStatement.setTimestamp(parameterIndex, x);

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		pStatement.setAsciiStream(parameterIndex, x, length);

	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		pStatement.setUnicodeStream(parameterIndex, x, length);

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		pStatement.setBinaryStream(parameterIndex, x, length);

	}

	@Override
	public void clearParameters() throws SQLException {
		pStatement.clearParameters();

	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		pStatement.setObject(parameterIndex, x, targetSqlType);

	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		pStatement.setObject(parameterIndex, x);

	}

	@Override
	public boolean execute() throws SQLException {
		return pStatement.execute();
	}

	@Override
	public void addBatch() throws SQLException {
		pStatement.addBatch();

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return pStatement.getMetaData();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		pStatement.setDate(parameterIndex, x, cal);

	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		pStatement.setTime(parameterIndex, x, cal);

	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

}
