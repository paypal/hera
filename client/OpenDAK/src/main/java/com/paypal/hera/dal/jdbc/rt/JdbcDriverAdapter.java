package com.paypal.hera.dal.jdbc.rt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import com.paypal.hera.cal.CalTransaction;

public interface JdbcDriverAdapter {

	/**
	 * Implements driver specific hacks for setString
	 */
	public void setStringParameter(PreparedStatement stmt,
		int index, String value, boolean isUtf8Db) throws SQLException;

	/**
	 * Implements driver specific hacks for setBytes
	 */
	public void setBytesParameter(PreparedStatement stmt,
		int index, byte[] value) throws SQLException;

	/**
	 * Indicates if the connection supports setting of isolation levels
	 * (Oracle: yes, HSQL: no)
	 */
	public boolean supportsTransactionIsolation();


	/**
	 * Checks whether retry on connect is expected for I/O errors
	 */
	public boolean expectsRetryOnConnectIoException(
		boolean forceRetryOnIoException);

	/**
	 * Should the given SQLException cause a pool flush
	 */
	public boolean shouldCausePoolFlush(SQLException sqle);


	
	/**
	 * 
	 * @param dbSessionParameters - HashMap of
	 * 	connection parameter and value (parameter is of type
	 *  DbSessionParameterEnum..
	 * 
	 * @return List of database specific AlterDbSessionParameter
	 * 
	 */
	public ArrayList getDbSessionParameterList(
		HashMap dbSessionParameters);

	/**
	 * Implements driver specific hacks for setTimestamp
	 */
	public void setTimestampParameter(PreparedStatement stmt, int index,
			Timestamp value) throws SQLException;
}
