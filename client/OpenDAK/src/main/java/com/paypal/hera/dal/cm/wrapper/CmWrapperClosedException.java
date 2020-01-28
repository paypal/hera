package com.paypal.hera.dal.cm.wrapper;

import java.sql.SQLException;

/**
 * Exception indicates that CmWrapper has been closed
 * 
 */
public class CmWrapperClosedException extends SQLException {

	public CmWrapperClosedException(String msg) {
		super(msg);
	}
}
