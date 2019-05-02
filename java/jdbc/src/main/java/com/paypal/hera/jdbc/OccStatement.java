package com.paypal.hera.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.paypal.hera.ex.OccExceptionBase;
import com.paypal.hera.ex.OccIOException;
import com.paypal.hera.ex.OccRuntimeException;
import com.paypal.hera.ex.OccSQLException;
import com.paypal.hera.ex.OccTimeoutException;
import com.paypal.hera.util.OccColumnMeta;
import com.paypal.hera.util.OccStatementsCache;
import com.paypal.hera.util.OccStatementsCache.StatementType;

public class OccStatement implements Statement {

	protected OccConnection connection; 
	protected OccResultSet resultSet;
	protected OccStatementsCache.StatementCacheEntry stCache;
	
	private int maxRows;
	protected int fetchSize;
	private boolean escapeProcessingEnabled;
	private boolean updateCountRetrieved;
	
	public OccStatement(OccConnection occConnection) {
		if (occConnection == null)
			throw new NullPointerException("No occ connection");
		connection = occConnection;
		resultSet = null;
		fetchSize = 0; // all rows
		escapeProcessingEnabled = connection.enableEscape();
	}
	
	ArrayList<OccColumnMeta> getColumnMeta() {
		return stCache.getColumnMeta();
	}

	 HashMap<String, Integer> getColumnIndexes() {
		return stCache.getColumnIndexes();
	}

	protected OccConnection checkOpened() throws OccRuntimeException {
		if (connection == null)
			throw new OccRuntimeException("OccStatement is closed");
		return connection;
	}
	
	public void close() throws OccExceptionBase {
		if (connection !=null) {
			connection.unregister(this);
			connection = null;
			if (resultSet != null) {
				resultSet.close();
				resultSet = null;
			}
		}
	}
	
	protected void prepare(String _sql) throws OccExceptionBase {
		stCache = connection.getStatementCache().getEntry(_sql, escapeProcessingEnabled, 
				connection.shardingEnabled(), connection.paramNameBindingEnabled());
		connection.getOccClient().prepare(stCache.getParsedSQL());
	}
	
	protected OccResultSet createRecordSet() throws SQLException {
		if (resultSet != null) {
			resultSet.close();
		}
		resultSet = new OccResultSet(connection, this, connection.getOccClient(), fetchSize);
		connection.setActiveResultSet(resultSet);
		return resultSet;
	}
	
	protected void helperInitExecute() throws OccExceptionBase {
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}
		OccResultSet rs = connection.getActiveResultSet();
		if (rs != null) {
			rs.fetchAllData();
			connection.setActiveResultSet(null);
		}
		updateCountRetrieved = false;
	}

	protected boolean helperExecute(boolean _add_commit) throws SQLException {

		switch (stCache.getStatementType()) {
		case DML:
			helperExecuteUpdate(_add_commit);
			return false;
		case NON_DML:
			helperExecuteQuery();
			return true;
		default:
			break;
		}

		try {
			boolean nonDML = connection.getOccClient().execute(fetchSize, _add_commit);
			if(nonDML) {
				ArrayList<OccColumnMeta> columnMeta = connection.getOccClient().getColumnMeta();
				stCache.setColumnMeta(columnMeta);
				createRecordSet();
			}
			stCache.setStatementType(nonDML ? StatementType.NON_DML : StatementType.DML);
			return nonDML;

		} catch(OccIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(OccTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}

	}
	
	protected OccResultSet helperExecuteQuery() throws SQLException {
		try {
			if (stCache.getColumnMeta() == null)
				stCache.setColumnMeta(connection.getOccClient().execQuery(fetchSize, true));
			else
				connection.getOccClient().execQuery(fetchSize, false);
		} catch(OccIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(OccTimeoutException ex) {
			connection.hardClose();
			throw ex;
		} 
		return createRecordSet();
	}
	
	protected int helperExecuteUpdate(boolean _add_commit) throws SQLException {
		try {
			connection.getOccClient().execDML(_add_commit);
		} catch(OccIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(OccTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}
		return connection.getOccClient().getRows();
	}
	
	public Connection getConnection()  throws SQLException {
		checkOpened();
		return connection;
	}

	public int getMaxFieldSize() throws SQLException {
		checkOpened();
		return 0;
	}

	public void setMaxFieldSize(int max) throws SQLException {
		checkOpened();
	}

	public int getMaxRows() throws SQLException {
		checkOpened();
		return maxRows;
	}

	public void setMaxRows(int max) throws SQLException {
		checkOpened();
		maxRows = max;
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkOpened();
		escapeProcessingEnabled = enable;
	}

	public void setFetchDirection(int direction) throws SQLException {
		checkOpened();
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new OccSQLException("Occ resultset is forward-only");
		}
	}

	public int getFetchDirection() throws SQLException {
		checkOpened();
		return ResultSet.FETCH_FORWARD;
	}

	public int getResultSetConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	public int getResultSetType()  throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	public void setFetchSize(int rows) throws SQLException {
		checkOpened();
		if ((rows != 0/*0 means all*/) && (rows < connection.getMinFetchSize()))
			rows = connection.getMinFetchSize();
		fetchSize = rows;
	}
  
	public int getFetchSize() throws SQLException {
		checkOpened();
		return fetchSize;
	}

	public int getQueryTimeout() throws SQLException {
		checkOpened();
		return 0;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		checkOpened();
	}

	public void cancel() throws SQLException {
		checkOpened();
		throw new OccSQLException("Statement execution cannot be cancelled");
	}

	public void setCursorName(String name) throws SQLException {
		checkOpened();
		throw new OccSQLException("ResutSet updates are not supported");
	}

	public SQLWarning getWarnings() throws SQLException {
		checkOpened();
		return null;
	}

	public void clearWarnings() throws SQLException {
		checkOpened();
	}

	public boolean execute(String sql) throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare(sql);
		return helperExecute(connection.getAutoCommit());
		
	}

	public ResultSet getResultSet() throws SQLException {
		checkOpened();
		return resultSet;
	}

	public int getUpdateCount() throws SQLException { 
		checkOpened();
		if (updateCountRetrieved)
			return -1;
		updateCountRetrieved = true;
		return connection.getOccClient().getRows();
	}

	public boolean getMoreResults() throws SQLException {
		checkOpened();
		return false;
	}

	public void addBatch(String sql) throws SQLException {
		checkOpened();
		throw new OccSQLException("Batch not supported");
	}

	public void clearBatch() throws SQLException {
		checkOpened();
		// don't throw exception, because of bug in ORM (i.e. DAL) which calls this even
		// if the bach was not used or supported
		// throw new OccSQLException("Batch not supported");
	}

	public int[] executeBatch() throws SQLException {
		checkOpened();
		throw new OccSQLException("Batch not supported");
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare(sql);
		return helperExecuteQuery();
	}

	public int executeUpdate(String sql) throws SQLException {
		checkOpened();
		helperInitExecute();
		prepare(sql);
		return helperExecuteUpdate(connection.getAutoCommit());
	}
	
	// JDBC3.0 COMPATIBILITY

	protected final void notSupported() throws SQLException {
		checkOpened();
		throw new SQLFeatureNotSupportedException("Not supported on Occ statement");
	}

	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.getMoreResults is not implemented");
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.getGeneratedKeys is not implemented");
		
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.executeUpdate is not implemented");
		
	}

	public boolean execute(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("OCCStatement.execute is not implemented");
		
	}

	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.getResultSetHoldability is not implemented");
		
	}

	// JDBC 4.0 COMPATIBILITY
	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.isClosed is not implemented");
		
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.isPoolable is not implemented");
		
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.setPoolable is not implemented");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.isWrapperFor is not implemented");
		
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.unwrap is not implemented");
		
	}

	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.closeOnCompletion is not implemented");
	}

	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("OCCStatement.isCloseOnCompletion is not implemented");
		
	}
}
