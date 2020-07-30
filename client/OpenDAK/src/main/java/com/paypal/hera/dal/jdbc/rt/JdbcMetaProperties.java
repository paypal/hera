package com.paypal.hera.dal.jdbc.rt;

public class JdbcMetaProperties {

	private final boolean m_isCharSetUTF;

	public JdbcMetaProperties(boolean isCharSetUTF) {
		m_isCharSetUTF = isCharSetUTF;
	}

	public boolean isCharSetUTF() {
		return m_isCharSetUTF;
	}
}
