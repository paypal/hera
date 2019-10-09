package com.paypal.hera.jdbc;

public class LastInsertIdResultSet extends ResultSetAdapter {
	public LastInsertIdResultSet() {
	}

	private long id;
	void setLong(long id) {
		this.id = id;
	}	

	public long getLong(int col) {
		return id;
	}
}
