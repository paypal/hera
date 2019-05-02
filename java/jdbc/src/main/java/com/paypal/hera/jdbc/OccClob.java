package com.paypal.hera.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import com.paypal.hera.util.OccJdbcConverter;
import com.paypal.hera.util.OccJdbcUtil;

public class OccClob implements Clob {

	private String data;

	public OccClob(byte[] _data) {
		data = OccJdbcConverter.occ2String(_data);
	}

	public OccClob() {
		data = null;
	}

	public long length() throws SQLException {
		return data.length();
	}

	public String getSubString(long pos, int length) throws SQLException {
		// for efficiency only support getting all data. no benefit if chopping and copying
		if ((pos == 1) && (length == data.length()))
			return data;
		return data.substring((int)pos - 1, (int)(pos + length - 1));
	}

	public Reader getCharacterStream() throws SQLException {
		return new StringReader(data);
	}

	public InputStream getAsciiStream() throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.getAsciiStream is not implemented");

	}

	public long position(String searchstr, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.position is not implemented");

	}

	public long position(Clob searchstr, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.position is not implemented");

	}

	// JDBC 3.0 SUPPORT

	public int setString(long pos, String str) throws SQLException {
		if (pos == 1) {
			data = str;
			return data.length();
		}
		OccJdbcUtil.notSupported("Clob supports only writing one chunk");
		return 0;
	}

	public int setString(long pos, String str, int offset, int len)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Clob.setString is not implemented");

	}

	public OutputStream setAsciiStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.setAsciiStream is not implemented");

	}

	public Writer setCharacterStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.setCharacterStream is not implemented");

	}

	public void truncate(long len) throws SQLException {
		OccJdbcUtil.notSupported("Clob.truncate is not implemented");
	}

	// JDBC 4.0
	public void free() throws SQLException {
		data = null;
	}

	public Reader getCharacterStream(long pos, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Clob.getCharacterStream is not implemented");

	}
}
