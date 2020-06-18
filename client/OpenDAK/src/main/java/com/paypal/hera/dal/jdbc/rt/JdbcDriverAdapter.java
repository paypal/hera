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
	 * Retrieves the database meta properties
	 */
	public JdbcMetaProperties getMetaProperties(Connection con) throws SQLException;


	/**
	 * Retrieves internal record sizes for buffers.
	 * We are using reflection to get to Oracle's internal buffers, and, hence,
	 * depend on their internal implementation
	 * 
	 * @param realStmt
	 * @return
	 * @throws DalRuntimeException
	 */
	public int getRecordSize(Statement realStmt) throws RuntimeException;

	/**
	 * Performs a dummy SQL on the backend database to test reacheability.
	 * 
	 * @throws SQLException is ping failed
	 */
	public void ping(Connection conn) throws SQLException;

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
	 * Indicates if the connection supports setting application info
	 */
	public boolean supportsApplicationInfo();

	/**
	 * Sets connection application info
	 */
	public void setApplicationInfo(Connection conn, String appInfo)
		throws SQLException;

	/**
	 * Checks whether exception represents unique constraint violation
	 */
	public boolean isUniqueConstraintException(SQLException sqle);

	/**
	 * Checks whether exception is because of bad user data and 
	 * requires data values to be logged. 
	 */
	public boolean isBadUserDataException(SQLException sqle);

	/**
	 * Check if SQLException represents a server-side error for re-trials
	 */
	public boolean isServerSideError(SQLException sqle);

	/**
	 * Check if SQLException represents a resource allocation
	 * error for re-trials
	 */
	public boolean shouldRetryOnConnect(SQLException sqle,
		boolean forceRetryOnIoException);

	/**
	 * Checks whether retry on connect is expected for I/O errors
	 */
	public boolean expectsRetryOnConnectIoException(
		boolean forceRetryOnIoException);

	/**
	 * Should the given SQLException count towards markdown
	 */
	public boolean shouldCountTowardsMarkdown(SQLException sqle);

	/**
	 * Should the given SQLException be considered a markdown
	 * without counting towards markdown
	 */
	public boolean shouldReportAsMarkdown(SQLException sqle);

	/**
	 * Should the given SQLException cause single connection flush
	 */
	public boolean shouldCauseConnectionFlush(SQLException sqle);

	/**
	 * Should the given SQLException cause a pool flush
	 */
	public boolean shouldCausePoolFlush(SQLException sqle);

	/**
	 * Returns description of the effective flush mode
	 */
	public String getAutoFlushTypeDescription();

	/**
	 * Check whether it's safe to repeat operation
	 */
	public boolean isRetryableException(SQLException sqle,
		boolean isSelectQuery);

	/**
	 * Checks whether exception represents a server-side connection error
	 * 
	 * This will return true if SQLException happened when DBIT server
	 * was trying to establish a connection to the Oracle database
	 */
	public boolean isServerConnectError(SQLException sqle);

	/**
	 * Checks whether exception represents a server-side connection timeout
	 * 
	 * This will return true if SQLException happened when DBIT server
	 * was trying to get a connection from its connection pool
	 */
	public boolean isServerConnectTimeout(SQLException sqle);

	/**
	 * Should the given SQLException be logged in CAL
	 */
	public boolean shouldLogInCal(SQLException sqle);

	/**
	 * Checks whether connection rollback after exception is needed
	 */
	public boolean needsRollbackAfterException(Connection conn,
		JdbcOperationType opType, SQLException e);

	/**
	 * Indicates if the connection supports getting last server-side duration
	 */
	public boolean supportsLastDbDuration();

	/**
	 * Gets last server-side duration, or -1 if it's unknown
	 */
	public float getLastDbDuration();

	/**
	 * Clears last server-side duration
	 */
	public void clearLastDbDuration();

	/**
	 * Returns server name
	 */
	public String getServerName(Connection conn) throws SQLException;

	/**
	 * Sets Oracle-specific plsql indexbytable parameter
	 */
	public void setPlsqlIndexTableStrings(PreparedStatement stmt,
		int bindOffset, String[] value, int maxLength)
		throws SQLException;

	/**
	 * Sets Oracle-specific plsql indexbytable parameter
	 */
	public void setPlsqlIndexTableNumbers(PreparedStatement stmt,
		int bindOffset, Object value)
		throws SQLException;

	/**
	 * Checks whether driver can set id for the next operation on the statement
	 * 
	 * This is useful when correlation between client-side and
	 * server-side activity is needed
	 */
	public boolean supportsRequestId();

	/**
	 * Sets id for the next operation on the statement and return it
	 * 
	 * This is useful when correlation between client-side and
	 * server-side activity is needed
	 */
	public long setRequestId(Statement stmt)
		throws SQLException;

	/**
	 * Returns a JdbcDriverInfo object containing the underlying driver info
	 */
	public JdbcDriverInfo getJdbcDriverInfo();
	
	/**
	 * Extracts the Database Host machine name/IP from a jdbc conenction
	 * URL.
	 */
	public String getDatabaseHost(String jdbcUrl);
	
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
	 * Prepares new connection for use by the application
	 * 
	 * This call will set application info on DB session (if possible),
	 * and it will return information about the connection and database itself
	 */	
	public JdbcConnectionInfo setupNewConnection(Connection conn, String appInfo)
	throws SQLException;
	
	/**
	 * 
	 * @return true if queryType is supported.
	 */
	public boolean supportsQueryType(String queryType);
	
	/**
	 * 
	 * @return true if autoIncrement is supported.
	 */
	public boolean supportsAutoIncrement();

	/**
	 * Clear or restore any large buffers held by the statement
	 * @param stmt
	 * @param shouldRestore
	 * @param restoreToClose
	 * @return the sizes of the bytebuffer & charbuffer used by the statement
	 */
	public String cleanStatementBuffers(Statement stmt, boolean shouldRestore, boolean restoreToClose);

	/**
	 * Adds properties necessary to enable statement freeing during caching
	 * @param addProperties
	 * @param singleUseBuffer
	 * @return
	 */
	public boolean addCleanStatementBuffersProp(Properties addProperties, boolean singleUseBuffer);
	
	/**
	 * Implements driver specific hacks for setTimestamp
	 */
	public void setTimestampParameter(PreparedStatement stmt, int index,
			Timestamp value) throws SQLException;
	
	public boolean isDbPasswordWrong(SQLException sqle);
	
	public void addExtraProps(Properties addProps, Properties existingConnProps);
	
	/**
	 * Replace the port part of JDBC Url into a new one.  
	 * 
	 * @param url, original JDBC Url
	 * @param port, the new port
	 * @return updated url after the port replacement.
	 */
	public String setupPortInUrl(String url, String port);
	
	/**
	 * Returns a CAL transaction object. It will return NullCalTransaction for occ drivers
	 * @param string name for CAL transaction type like EXEC, FETCH and such
	 * @return CAL Transaction object for given caltype
	 */
	public CalTransaction getCalTransaction(String calType);
	
	/**
	 * Logs a cal event for exception. This will be no-op for non-occ drivers
	 * * @param exception to be logged in CAL event
	 * @return void
	 */
	public void writeExceptionEventToCal(Exception e);

	/**
	 * Logs sql to CAL. Returns sql hash for sql statement string
	 * @param string sql statement string
	 * @return sql hash of statement passed as input
	 */
	public long writeSqlToCal(String sql);
}
