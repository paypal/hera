package com.paypal.hera.dal.jdbc.rt;

/**
 * JdbcDriverInfo acts as a container for JDBC driver information
 * that is displayed on a ValidateInternals page.
 * 
 * During startup, the JdbcDriverInfoFactory initializes an instance
 * of this class with Driver information and holds onto it so that
 * a call to getJdbcDriverInfo does not hit the database; it simply
 * returns the cached data.
 * 
 */
public class JdbcDriverInfo {

	private String m_driverName;
	private String m_driverVersion;
	private String m_lastModifiedUser;
	private String m_lastModifiedDate;
	private String[] m_fixes;

	/**
	 * constructor for JdbcDriverInfo
	 */
	// @PMD:REVIEWED:ExcessiveParameterList: by ichernyshev on 09/02/05
	public JdbcDriverInfo(String driverName, String driverVersion,
		String lastModifiedUser, String lastModifiedDate,
		String[] fixes)
	{
		m_driverName = driverName;
		m_driverVersion = driverVersion;
		m_lastModifiedUser = lastModifiedUser;
		m_lastModifiedDate = lastModifiedDate;
		m_fixes = fixes;
	}

	public JdbcDriverInfo(String driverName, String driverVersion)
	{
		m_driverName = driverName;
		m_driverVersion = driverVersion;
	}

	/**
	 * @return String the name of this JDBC driver
	 */
	public String getDriverName() {
		return m_driverName;
	}

	/**
	 * @return String the version of this JDBC driver
	 */
	public String getDriverVersion() {
		return m_driverVersion;
	}

	public String getLastModifiedUser() {
		return m_lastModifiedUser;
	}

	public String getLastModifiedDate() {
		return m_lastModifiedDate;
	}

	public String[] getFixes() {
		return m_fixes;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(200);

		sb.append("JdbcDriverInfo[driverName = ");
		sb.append(m_driverName);
		sb.append(", driverVersion = ");
		sb.append(m_driverVersion);
		sb.append(", lastModifiedUser = ");
		sb.append(m_lastModifiedUser);
		sb.append(", lastModifiedDate = ");
		sb.append(m_lastModifiedDate);
		sb.append(", fixes = {");
		if (m_fixes != null) {
			for (int i=0; i<m_fixes.length; i++) {
				if (i != 0) {
					sb.append(",");
				}
				sb.append(m_fixes[i]);
			}
		} else {
			sb.append("null");
		}
		sb.append("}]");

		return sb.toString();
	}
}
