package com.paypal.hera.jdbc;

import java.sql.*;

import com.paypal.hera.conf.HeraClientConfigHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeraDatabaseMetadata implements DatabaseMetaData {
	final Logger LOGGER = LoggerFactory.getLogger(HeraDatabaseMetadata.class);
	
	private HeraConnection connection;
	
	public HeraDatabaseMetadata(HeraConnection _connection) {
		connection = _connection;
	}
	/*** instead of throwing an exception, which will fail hibernate, we just log an debug info, only when at debug mode 
	 *   logging in debug mode only so we will not impact performance during non-debug mode
	 * @throws SQLException if not connected
	 */
	protected final void logNoImplementationInDebug() throws SQLException{
		connection.checkOpened();

		if (LOGGER.isDebugEnabled()) {
			/* figure out which method calls this method ***/
			StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
			StackTraceElement e = stacktrace[2];
			String methodName = e.getMethodName();
			String  className = e.getClassName();
			className = className.substring(className.lastIndexOf(".")+1);

			String fullMethodName = className + "." + methodName + "()";

			/* the  log msg is like "HeraDatabaseMetadata.getDatabaseMajorVersion(): Not ...." */
			LOGGER.debug(fullMethodName + ": Not supported on Hera DatabaseMetadata. the returned result may not be correct. Fully implementation is needed. call it at your own risk");
		}
	}

	public Connection getConnection() throws SQLException {
		connection.checkOpened();
		return connection;
	}

	public String getURL() throws SQLException {
		return connection.getUrl();
	}

	public String getUserName() throws SQLException {
		return "";
	}

	public String getDatabaseProductName() throws SQLException {
		connection.checkOpened();
		return connection.getDataSource().name();
	}

	public String getDatabaseProductVersion() throws SQLException {
		connection.checkOpened();
		if(connection.getDataSource().equals(HeraClientConfigHolder.E_DATASOURCE_TYPE.MySQL)) {
			PreparedStatement pst = connection.prepareStatement("select @@version");
			ResultSet rs = pst.executeQuery();
			if (rs.next()) {
				return rs.getString(1);
			}
		}
		return HeraClientConfigHolder.E_DATASOURCE_TYPE.HERA + " v 1.0";
	}

	public String getDriverName() throws SQLException {
		connection.checkOpened();
		return HeraDriver.DRIVER_NAME;
	}

	public String getDriverVersion() throws SQLException {
		connection.checkOpened();
		return "v" + HeraDriver.DRIVER_MAJOR_VERSION + "." + HeraDriver.DRIVER_MINOR_VERSION;
	}

	public int getDriverMajorVersion() {
		return HeraDriver.DRIVER_MAJOR_VERSION;
	}

	public int getDriverMinorVersion() {
		return HeraDriver.DRIVER_MINOR_VERSION;
	}

	public boolean allProceduresAreCallable() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean allTablesAreSelectable() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean isReadOnly() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean nullsAreSortedHigh() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean nullsAreSortedLow() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean nullsAreSortedAtStart() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean nullsAreSortedAtEnd() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean usesLocalFiles() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean usesLocalFilePerTable() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesUpperCaseIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesLowerCaseIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesMixedCaseIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public String getIdentifierQuoteString() throws SQLException {
		connection.checkOpened();
		return "\"";
	}

	public String getSQLKeywords() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public String getNumericFunctions() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public String getStringFunctions() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public String getSystemFunctions() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public String getTimeDateFunctions() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public String getSearchStringEscape() throws SQLException {
		connection.checkOpened();
		return "\\";
	}

	public String getExtraNameCharacters() throws SQLException {
		connection.checkOpened();
		return "";
	}

	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsColumnAliasing() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean nullPlusNonNullIsNull() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsConvert() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsConvert(int fromType, int toType)
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean supportsTableCorrelationNames() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsDifferentTableCorrelationNames()
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean supportsExpressionsInOrderBy() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsOrderByUnrelated() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsGroupBy() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsGroupByUnrelated() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsGroupByBeyondSelect() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsLikeEscapeClause() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsMultipleResultSets() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsMultipleTransactions() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsNonNullableColumns() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsMinimumSQLGrammar() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsCoreSQLGrammar() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsExtendedSQLGrammar() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsANSI92FullSQL() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsOuterJoins() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsFullOuterJoins() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsLimitedOuterJoins() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public String getSchemaTerm() throws SQLException {
		connection.checkOpened();
		return "schema";
	}

	public String getProcedureTerm() throws SQLException {
		connection.checkOpened();
		return "procedure";
	}

	public String getCatalogTerm() throws SQLException {
		connection.checkOpened();
		return "catalog";
	}

	public boolean isCatalogAtStart() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public String getCatalogSeparator() throws SQLException {
		connection.checkOpened();
		return ".";
	}

	public boolean supportsSchemasInDataManipulation() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSchemasInPrivilegeDefinitions()
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsCatalogsInPrivilegeDefinitions()
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean supportsPositionedDelete() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsPositionedUpdate() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsSelectForUpdate() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsStoredProcedures() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSubqueriesInComparisons() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSubqueriesInExists() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSubqueriesInIns() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsCorrelatedSubqueries() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsUnion() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsUnionAll() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public int getMaxBinaryLiteralLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxCharLiteralLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnsInGroupBy() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnsInIndex() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnsInOrderBy() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnsInSelect() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxColumnsInTable() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxConnections() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxCursorNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxIndexLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxSchemaNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxProcedureNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxCatalogNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxRowSize() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public int getMaxStatementLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxStatements() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxTableNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxTablesInSelect() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getMaxUserNameLength() throws SQLException {
		connection.checkOpened();
		return 0;
	}

	public int getDefaultTransactionIsolation() throws SQLException {
		connection.checkOpened();
		return Connection.TRANSACTION_READ_COMMITTED;
	}

	public boolean supportsTransactions() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsTransactionIsolationLevel(int level)
		throws SQLException
	{
		connection.checkOpened();
		return (level == Connection.TRANSACTION_READ_COMMITTED);
	}

	public boolean supportsDataDefinitionAndDataManipulationTransactions()
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean supportsDataManipulationTransactionsOnly()
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public boolean dataDefinitionCausesTransactionCommit()
		throws SQLException
	{
		connection.checkOpened();
		return true;
	}

	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public ResultSet getProcedures(
		String catalog,
		String schemaPattern,
		String procedureNamePattern)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getProcedureColumns(
		String catalog,
		String schemaPattern,
		String procedureNamePattern,
		String columnNamePattern)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getTables(
		String catalog,
		String schemaPattern,
		String tableNamePattern,
		String[] types)
		throws SQLException
	{
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getSchemas() throws SQLException {
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getCatalogs() throws SQLException {
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getTableTypes() throws SQLException {
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getColumns(
		String catalog,
		String schemaPattern,
		String tableNamePattern,
		String columnNamePattern)
		throws SQLException
	{
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getColumnPrivileges(
		String catalog,
		String schema,
		String table,
		String columnNamePattern)
		throws SQLException
	{
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getTablePrivileges(
		String catalog,
		String schemaPattern,
		String tableNamePattern)
		throws SQLException
	{
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		logNoImplementationInDebug();
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getBestRowIdentifier(
		String catalog,
		String schema,
		String table,
		int scope,
		boolean nullable)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getVersionColumns(
		String catalog,
		String schema,
		String table)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getPrimaryKeys(
		String catalog,
		String schema,
		String table)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getImportedKeys(
		String catalog,
		String schema,
		String table)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getExportedKeys(
		String catalog,
		String schema,
		String table)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getCrossReference(
		String primaryCatalog,
		String primarySchema,
		String primaryTable,
		String foreignCatalog,
		String foreignSchema,
		String foreignTable)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getTypeInfo() throws SQLException {
		logNoImplementationInDebug();
		/*** return an empty rs to fool hibernate. a full implementation
		 * should be added later by contacting the hera server&db
		 */
		HeraResultSet rs = new HeraResultSet();
		return rs;
	}

	public ResultSet getIndexInfo(
		String catalog,
		String schema,
		String table,
		boolean unique,
		boolean approximate)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public boolean supportsResultSetType(int type) throws SQLException {
		connection.checkOpened();
		return (type == ResultSet.TYPE_FORWARD_ONLY);
	}

	public boolean supportsResultSetConcurrency(int type, int concurrency)
		throws SQLException
	{
		connection.checkOpened();
		return (type == ResultSet.TYPE_FORWARD_ONLY &&
			concurrency == ResultSet.CONCUR_READ_ONLY);
	}

	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean ownDeletesAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean ownInsertsAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean othersDeletesAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean othersInsertsAreVisible(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean updatesAreDetected(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean deletesAreDetected(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean insertsAreDetected(int type) throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsBatchUpdates() throws SQLException {
		connection.checkOpened();
		return true;
	}

	public ResultSet getUDTs(
		String catalog,
		String schemaPattern,
		String typeNamePattern,
		int[] types)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	// JDBC 3.0 SUPPORT

	public boolean supportsSavepoints() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsNamedParameters() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsMultipleOpenResults() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsGetGeneratedKeys() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public ResultSet getSuperTypes(String catalog, String schemaPattern, 
		String typeNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getSuperTables(String catalog, String schemaPattern,
		String tableNamePattern) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getAttributes(String catalog, String schemaPattern,
		String typeNamePattern, String attributeNamePattern) 
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	public boolean supportsResultSetHoldability(int holdability)
		throws SQLException
	{
		connection.checkOpened();
		return false;
	}

	public int getResultSetHoldability() throws SQLException {
		connection.checkOpened();
		return 2;
		//return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	public int getDatabaseMajorVersion() throws SQLException {
		return 11;
	}

	public int getDatabaseMinorVersion() throws SQLException {
		return 2;
	}

	public int getJDBCMajorVersion() throws SQLException {
		return 4;
	}

	public int getJDBCMinorVersion() throws SQLException {
		return 1;
	}

	public int getSQLStateType() throws SQLException {
		connection.checkOpened();
		return 1; // sqlStateXOpen
	}

	public boolean locatorsUpdateCopy() throws SQLException {
		connection.checkOpened();
		return false;
	}

	public boolean supportsStatementPooling() throws SQLException {
		connection.checkOpened();
		return true;
	}

	// JDBC 4.0
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public ResultSet getPseudoColumns(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public boolean generatedKeyAlwaysReturned() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}
}
