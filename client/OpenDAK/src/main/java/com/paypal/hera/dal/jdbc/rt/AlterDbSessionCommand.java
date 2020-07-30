/*
 * Created on Sep 19, 2006
 *
 */
package com.paypal.hera.dal.jdbc.rt;

import java.sql.Connection;
import java.sql.SQLException;

public interface AlterDbSessionCommand {
	/**
	 * This method is called when new CmConnectionWrapper is created by 
	 * ConnectionPool.
	 * 
	 * @param connection database connection
	 * 
	 * @throws SQLException
	 */
	void setParameterValue(Connection connection) throws SQLException;

	/**
	 * During CmConnectionWrapper.close(), this method is called to reset
	 * connection parameter values back to original.
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	void revertToOriginal(Connection connection) throws SQLException;
}
