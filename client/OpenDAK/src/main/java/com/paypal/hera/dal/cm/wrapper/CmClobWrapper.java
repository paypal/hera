package com.paypal.hera.dal.cm.wrapper;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 *
 * Wraps Clob to allow custom processing of exceptions and to control connection
 * 
 * 
 */
public class CmClobWrapper extends CmBaseWrapper implements Clob
{
	protected final CmConnectionWrapper m_connection;
	private Clob m_clob;

	CmClobWrapper(CmConnectionWrapper connection, Clob clob)
	{
		super(connection);
		m_connection = connection;
		m_clob = clob;
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

	static Clob unwrap(Clob clob)
	{
		if (!(clob instanceof CmClobWrapper)) {
			return clob;
		}

		return ((CmClobWrapper)clob).m_clob;
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
			m_clob = null;
		}
	}
	
	/**
	 * @return
	 * @throws java.sql.SQLException
	 */
	public InputStream getAsciiStream() throws SQLException {
		checkOpened();

		InputStream result = null;
		JdbcOperationType opType = JdbcOperationType.LOB_GET_STREAM;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_clob.getAsciiStream();
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

	/**
	 * @return
	 * @throws java.sql.SQLException
	 */
	public Reader getCharacterStream() throws SQLException
	{
		checkOpened();

		Reader result = null;
		JdbcOperationType opType = JdbcOperationType.LOB_GET_STREAM;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_clob.getCharacterStream();
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

	/**
	 * @param pos
	 * @param length
	 * @return
	 * @throws java.sql.SQLException
	 */
	public String getSubString(long pos, int length) throws SQLException
	{
		checkOpened();

		String result = null;
		JdbcOperationType opType = JdbcOperationType.LOB_GET_DATA;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_clob.getSubString(pos, length);
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

	/**
	 * @return
	 * @throws java.sql.SQLException
	 */
	public long length() throws SQLException {
		checkOpened();
		return m_clob.length();
	}

	/**
	 * @param searchstr
	 * @param start
	 * @return
	 * @throws java.sql.SQLException
	 */
	public long position(String searchstr, long start) throws SQLException
	{
		checkOpened();

		long result = 0;
		JdbcOperationType opType = JdbcOperationType.LOB_POSITION;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_clob.position(searchstr, start);
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

	/**
	 * @param searchstr
	 * @param start
	 * @return
	 * @throws java.sql.SQLException
	 */
	public long position(Clob searchstr, long start) throws SQLException
	{
		checkOpened();

		long result = 0;
		JdbcOperationType opType = JdbcOperationType.LOB_POSITION;
		SQLException sqlexception = null;
		synchronized (getLock())
		{
			try {
				cmCallStart(opType);
				result = m_clob.position(searchstr, start);
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
	
	public OutputStream setAsciiStream(long pos) throws SQLException {
		checkOpened();
		return m_clob.setAsciiStream(pos);
	}
	
	public Writer setCharacterStream(long pos) throws SQLException {
		checkOpened();
		return m_clob.setCharacterStream(pos);
	}
		
	public int setString(long pos, String str) throws SQLException {
		checkOpened();
		return m_clob.setString(pos, str);
	}

	public int setString(long pos, String str, int offset, int len) throws SQLException {
		checkOpened();
		return m_clob.setString(pos, str, offset, len);
	}
	
	public void truncate(long len) throws SQLException {
		checkOpened();
		m_clob.truncate(len);
	}

	public void free() throws SQLException {
		throw new UnsupportedOperationException("Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.");
	}

	public Reader getCharacterStream(long pos, long length) throws SQLException {
		throw new UnsupportedOperationException("Not Implemented - Required for JDBC 4.0 / JDK 1.6 compliance.");
	}
}
