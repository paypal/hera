package com.paypal.hera.dal.jdbc.rt;

/**
 * Keeps information about individual DB connection
 * 
 */
public class JdbcConnectionInfo {
	private String m_dbHost;
	private String m_sessionId;
	private String m_sessionId2;
	private boolean m_knowsUtfStatus;
	private boolean m_isUtf8;

	public JdbcConnectionInfo() {
	}

	public String getDbHost() {
		return m_dbHost;
	}

	public String getSessionId() {
		return m_sessionId;
	}

	public String getSessionId2() {
		return m_sessionId2;
	}

	public boolean knowsUtfStatus() {
		return m_knowsUtfStatus;
	}

	public boolean isUtf8() {
		return m_isUtf8;
	}

	public void setDbHost(String value) {
		m_dbHost = value;
	}

	public void setSessionId(String value) {
		m_sessionId = value;
	}

	public void setSessionId2(String value) {
		m_sessionId2 = value;
	}

	public void setUtf8(boolean value) {
		m_isUtf8 = value;
		m_knowsUtfStatus = true;
	}
}
