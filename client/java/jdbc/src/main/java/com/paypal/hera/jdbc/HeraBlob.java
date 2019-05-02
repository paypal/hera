package com.paypal.hera.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import com.paypal.hera.util.HeraJdbcUtil;

public class HeraBlob implements Blob {
	private byte[] data;

	public HeraBlob(byte[] _data) {
		data = _data;
	}

	public HeraBlob() {
		data = null;
	}

	public long length() throws SQLException {
		return data.length;
	}

	public byte[] getBytes(long pos, int length) throws SQLException {
		// for efficiency only support getting all data. no benefit if chopping and copying
		if ((pos == 1) && (length == data.length)) {
			return data;
		}
		throw new SQLFeatureNotSupportedException("Blob.getBytes only supports to get all data at once");
		/*
		if (pos < 1 || length < 0) {
			throw new ArrayIndexOutOfBoundsException();
		}

		pos--; // pos started with 1

		int resultSize = (int)(length() - pos);
		if (resultSize < 0) {
			throw new ArrayIndexOutOfBoundsException();
		}

		if (length < resultSize) {
			resultSize = length;
		}
		
		byte[] result = new byte[(int)resultSize];
		
		System.arraycopy(data, (int)pos, result, 0, resultSize);
		return result;
		*/
	}

	public InputStream getBinaryStream() throws SQLException {
		return new ByteArrayInputStream(data);
	}

	public long position(byte pattern[], long start) throws SQLException {
		throw new SQLFeatureNotSupportedException("Blob.position is not implemented");
	}

	public long position(Blob pattern, long start) throws SQLException {
		throw new SQLFeatureNotSupportedException("Blob.position is not implemented");
	}

	// JDBC 3.0 SUPPORT
	
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		if (pos == 1) {
			data = java.util.Arrays.copyOf(bytes, bytes.length);
			return data.length;
		}
		throw new SQLFeatureNotSupportedException("Blob supports only writing one chunk");
	}

	public int setBytes(long pos, byte[] bytes, int offset, int len)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("Blob.setBytes is not implemented");
	}

	public OutputStream setBinaryStream(long pos) throws SQLException {
		throw new SQLFeatureNotSupportedException("Blob.setBinaryStream is not implemented");
	}

	public void truncate(long len) throws SQLException {
		throw new SQLFeatureNotSupportedException("Blob.truncate is not implemented");
	}

	// JDBC 4.0
	public void free() throws SQLException {
		data = null;
	}

	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException("Blob.getBinaryStream is not implemented");
	}
}
