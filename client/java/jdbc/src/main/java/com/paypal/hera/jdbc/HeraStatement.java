package com.paypal.hera.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.paypal.hera.conf.HeraClientConfigHolder.DATASOURCE_TYPE;
import com.paypal.hera.ex.HeraExceptionBase;
import com.paypal.hera.ex.HeraIOException;
import com.paypal.hera.ex.HeraRuntimeException;
import com.paypal.hera.ex.HeraSQLException;
import com.paypal.hera.ex.HeraTimeoutException;
import com.paypal.hera.util.HeraColumnMeta;
import com.paypal.hera.util.HeraStatementsCache;
import com.paypal.hera.util.HeraStatementsCache.StatementType;

public class HeraStatement implements Statement {

	protected HeraConnection connection; 
	protected HeraResultSet resultSet;
	protected HeraStatementsCache.StatementCacheEntry stCache;
	
	private int maxRows;
	protected int fetchSize;
	private boolean escapeProcessingEnabled;
	private boolean updateCountRetrieved;
	private DATASOURCE_TYPE datasource;
	
	public HeraStatement(HeraConnection heraConnection) {
		if (heraConnection == null)
			throw new NullPointerException("No hera connection");
		connection = heraConnection;
		resultSet = null;
		fetchSize = 0; // all rows
		escapeProcessingEnabled = connection.enableEscape();
		datasource = connection.getDataSource();
	}
	
	ArrayList<HeraColumnMeta> getColumnMeta() {
		return stCache.getColumnMeta();
	}

	 HashMap<String, Integer> getColumnIndexes() {
		return stCache.getColumnIndexes();
	}

	protected HeraConnection checkOpened() throws HeraRuntimeException {
		if (connection == null)
			throw new HeraRuntimeException("HeraStatement is closed");
		return connection;
	}
	
	public void close() throws HeraExceptionBase {
		if (connection !=null) {
			connection.unregister(this);
			connection = null;
			if (resultSet != null) {
				resultSet.close();
				resultSet = null;
			}
		}
	}
	
	protected void prepare(String _sql) throws HeraExceptionBase {
		stCache = connection.getStatementCache().getEntry(_sql, escapeProcessingEnabled, 
				connection.shardingEnabled(), connection.paramNameBindingEnabled(), datasource);
		connection.getHeraClient().prepare(stCache.getParsedSQL());
	}
	
	protected HeraResultSet createRecordSet() throws SQLException {
		if (resultSet != null) {
			resultSet.close();
		}
		resultSet = new HeraResultSet(connection, this, connection.getHeraClient(), fetchSize);
		connection.setActiveResultSet(resultSet);
		return resultSet;
	}
	
	protected void helperInitExecute() throws HeraExceptionBase {
		if (resultSet != null) {
			resultSet.close();
			resultSet = null;
		}
		HeraResultSet rs = connection.getActiveResultSet();
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
			boolean nonDML = connection.getHeraClient().execute(fetchSize, _add_commit);
			if(nonDML) {
				ArrayList<HeraColumnMeta> columnMeta = connection.getHeraClient().getColumnMeta();
				stCache.setColumnMeta(columnMeta);
				createRecordSet();
			}
			stCache.setStatementType(nonDML ? StatementType.NON_DML : StatementType.DML);
			return nonDML;

		} catch(HeraIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(HeraTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}

	}
	
	protected HeraResultSet helperExecuteQuery() throws SQLException {
		try {
			if (stCache.getColumnMeta() == null)
				stCache.setColumnMeta(connection.getHeraClient().execQuery(fetchSize, true));
			else
				connection.getHeraClient().execQuery(fetchSize, false);
		} catch(HeraIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(HeraTimeoutException ex) {
			connection.hardClose();
			throw ex;
		} 
		return createRecordSet();
	}
	
	protected int helperExecuteUpdate(boolean _add_commit) throws SQLException {
		try {
			connection.getHeraClient().execDML(_add_commit);
		} catch(HeraIOException ex) {
			connection.hardClose();
			throw ex;
		} catch(HeraTimeoutException ex) {
			connection.hardClose();
			throw ex;
		}
		return connection.getHeraClient().getRows();
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
			throw new HeraSQLException("Hera resultset is forward-only");
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
		throw new HeraSQLException("Statement execution cannot be cancelled");
	}

	public void setCursorName(String name) throws SQLException {
		checkOpened();
		throw new HeraSQLException("ResutSet updates are not supported");
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
		return connection.getHeraClient().getRows();
	}

	public boolean getMoreResults() throws SQLException {
		checkOpened();
		return false;
	}

	public void addBatch(String sql) throws SQLException {
		checkOpened();
		throw new HeraSQLException("Batch not supported");
	}

	public void clearBatch() throws SQLException {
		checkOpened();
		// don't throw exception, because of bug in ORM (i.e. DAL) which calls this even
		// if the bach was not used or supported
		// throw new HeraSQLException("Batch not supported");
	}

	public int[] executeBatch() throws SQLException {
		checkOpened();
		throw new HeraSQLException("Batch not supported");
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
		throw new SQLFeatureNotSupportedException("Not supported on Hera statement");
	}

	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.getMoreResults is not implemented");
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.getGeneratedKeys is not implemented");
		
	}

	public int executeUpdate(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.executeUpdate is not implemented");
		
	}

	public int executeUpdate(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.executeUpdate is not implemented");
		
	}

	public boolean execute(String sql, int autoGeneratedKeys)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, int[] columnIndexes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.execute is not implemented");
		
	}

	public boolean execute(String sql, String[] columnNames)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("HeraStatement.execute is not implemented");
		
	}

	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.getResultSetHoldability is not implemented");
		
	}

	// JDBC 4.0 COMPATIBILITY
	public boolean isClosed() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.isClosed is not implemented");
		
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.isPoolable is not implemented");
		
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.setPoolable is not implemented");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.isWrapperFor is not implemented");
		
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.unwrap is not implemented");
		
	}

	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.closeOnCompletion is not implemented");
	}

	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException("HeraStatement.isCloseOnCompletion is not implemented");
		
	}
}
