package com.paypal.hera.dal.cm.wrapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.paypal.hera.dal.jdbc.rt.JdbcOperationType;

/**
 * Callback mechanism to receive notifications of database operations
 * and perform custom exception processing
 * 
 */
public interface CmConnectionCallback {

	/**
	 * Invoked before a call to database is made
	 * 
	 * @param conn actual wrapped connection
	 * @param stmt actual wrapped statement or null
	 */
	public void cmCallStart(Connection conn, Statement stmt,
		JdbcOperationType opType) throws SQLException;

	/**
	 * Invoked after a call to database is made
	 * 
	 * @param conn actual wrapped connection
	 * @param stmt actual wrapped statement or null
	 * @param e exception thrown from the driver or null
	 */
	public void cmCallEnd(Connection conn, Statement stmt,
		JdbcOperationType opType, SQLException e);

	/**
	 * Invoked when exception has occured
	 * 
	 * @param conn actual wrapped connection
	 * @param stmt actual wrapped statement or null
	 * @param e exception thrown from the driver or null
	 * 
	 * @return exception to be thrown by the wrapper
	 */
	public SQLException cmProcessException(
		Connection conn, Statement stmt,
		JdbcOperationType opType, SQLException e);

	/**
	 * Invoked when proxy is closed
	 * 
	 * Implementer may choose to close the actual connection or
	 * do any other action, such as return connection to the pool
	 * 
	 * @param conn actual wrapped connection
	 */
	public void cmConnectionClose(Connection conn)
		throws SQLException;

	/**
	 * Invoked when proxy received runtime exception during
	 * close of child objects
	 * 
	 * Implementer has to close or discard the actual connection
	 * 
	 * @param conn actual wrapped connection
	 */
	public void cmConnectionDestory(Connection conn)
		throws SQLException;

	/**
	 * Returns true if database is UTF8
	 */
	public boolean cmIsUtf8Db() throws SQLException;
}
