package com.paypal.hera.dal.cm.wrapper;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 *
 * Wraps Blob to allow custom processing of exceptions and to control connection
 * 
 * 
 */
public class CmBlobWrapper extends CmBaseWrapper implements Blob
{
	protected final CmConnectionWrapper m_connection;
	private Blob m_blob;

	CmBlobWrapper(CmConnectionWrapper connection, Blob blob)
	{
		super(connection);
		m_connection = connection;
		m_blob = blob;
	}
	
	SQLException cmProcessException(
		JdbcOperationType opType, SQLException e)
	{
		return m_connection.cmProcessException(null, opType, e);
	}


	void cmCallStart(JdbcOperationType opType) throws SQLException
	{
		m_connection.cmCallStart(null, opType);
		startUse(opType);
	}

	void cmCallEnd(JdbcOperationType opType, SQLException e)
	{
		endUse(opType);
		m_connection.cmCallEnd(null, opType, e);
	}

	static Blob unwrap(Blob blob)
	{
		if (!(blob instanceof CmBlobWrapper)) {
			return blob;
		}

		return ((CmBlobWrapper)blob).m_blob;
	}

	public void close() throws SQLException
	{
		if (isClosed()) {
			return;
		}

		synchronized (getLock()) {
			closeInternal();
		}
	}

	public void parentClosed() throws SQLException
	{
		if (isClosed()) {
			return;
		}

		// do not synchronize as connection's close may be invoked
		// from a different thread
		closeInternal();
	}

	private void closeInternal() throws SQLException
	{
		if (isClosed()) {
			return;
		}

		try {
			super.close();
		} finally {
			m_blob = null;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see java.sql.Blob#getBinaryStream()
	 */
	public InputStream getBinaryStream() throws SQLException
	{
		checkOpened();

		InputStream result = null;
		JdbcOperationType opType = JdbcOperationType.LOB_GET_STREAM;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_blob.getBinaryStream();
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see java.sql.Blob#getBytes(long, int)
	 */
	public byte[] getBytes(long pos, int length) throws SQLException 
	{
		checkOpened();

		byte[] result = null;
		JdbcOperationType opType = JdbcOperationType.LOB_GET_DATA;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_blob.getBytes(pos, length);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see java.sql.Blob#length()
	 */
	public long length() throws SQLException
	{
		checkOpened();
		return m_blob.length();
	}

	/* (non-Javadoc)
	 * @see java.sql.Blob#position(java.sql.Blob, long)
	 */
	public long position(Blob pattern, long start) throws SQLException
	{
		checkOpened();

		long result = 0;
		JdbcOperationType opType = JdbcOperationType.LOB_POSITION;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_blob.position(pattern, start);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
		}
		return result;
	}

	/* (non-Javadoc)
	 * @see java.sql.Blob#position(byte[], long)
	 */
	public long position(byte[] pattern, long start) throws SQLException
	{
		checkOpened();

		long result = 0;
		JdbcOperationType opType = JdbcOperationType.LOB_POSITION;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_blob.position(pattern, start);
			} catch (SQLException e) {
				sqlexception = e;
			} finally {
				cmCallEnd(opType, sqlexception);
				if (sqlexception != null) {
					sqlexception = cmProcessException(opType, sqlexception);
					throw sqlexception;
				}
			}
		}
		return result;
	}

	/****** The following methods are required for JDK 1.5: ******/
	
	public OutputStream setBinaryStream(long pos) throws SQLException {
		checkOpened();
		return m_blob.setBinaryStream(pos);
	}
	
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		checkOpened();
		return m_blob.setBytes(pos, bytes);
	}

	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		checkOpened();
		return m_blob.setBytes(pos, bytes, offset, len);
	}
	
	public void truncate(long len) throws SQLException {
		checkOpened();
		m_blob.truncate(len);
	}

	public void free() throws SQLException {
		throw new UnsupportedOperationException("Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.");
	}

	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		throw new UnsupportedOperationException("Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.");
	}
}
