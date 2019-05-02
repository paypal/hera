package com.paypal.jmux.constants;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OccJdbcDriverConstants {

	public static final int DB_MACHINENAME_OFFSET_IN_URL = "jdbc:occ:".length();

	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on
	// 09/02/05
	private final Set<Object> s_staleConnErrors_priv = new HashSet<Object>();
	public Set<Object> s_staleConnErrors;
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on
	// 09/02/05
	private final Set s_markdownErrors_priv = new HashSet();
	public Set s_markdownErrors;
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on
	// 09/02/05
	private final Set s_serverSideErrors_priv = new HashSet();
	public Set s_serverSideErrors;
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by ichernyshev on
	// 09/02/05
	private final Set s_resourceAllocationErrors_priv = new HashSet();
	public Set s_resourceAllocationErrors;
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by rnagaraju on 09/07/05
	private final Set s_badUserDataErrors_priv = new HashSet();
	public Set s_badUserDataErrors;
	// @PMD:REVIEWED:EBayVariableNamingConventionsRule: by rnagaraju on 09/07/05
	private final Set s_ignorableErrors_priv = new HashSet();
	public Set s_ignorableErrors;
	// creating new hasset to markdown datasource if there was write permissions
	// error. There is a scope of refactoring the code by using s_markdownErrors
	// hasset instead of this newly created one.
	private final Set s_shouldReportAsMarkdownErrors_priv = new HashSet();
	public Set s_shouldReportAsMarkdownErrors;

	private final Set s_passwordWrongError_priv = new HashSet();
	public Set s_passwordWrongError;

	private final Map<Integer, List<String>> s_markdownErrorsWithSubstrings_priv = new HashMap<Integer, List<String>>();
	public Map<Integer, List<String>> s_markdownErrorsWithSubstrings;

	// use an error number in the range of 20000 to 20999, inclusive
	public static final int CUSTOM_ERR_MIN_CODE = 20000;
	public static final int CUSTOM_ERR_MAX_CODE = 20999;
	
	private static OccJdbcDriverConstants m_singleton = new OccJdbcDriverConstants();
	private OccJdbcDriverConstants() {
		initializeStatics();
	}
	
	public static OccJdbcDriverConstants getInstance() {
		return m_singleton;
	}

	private void initializeStatics() {
		// EXCEPTIONS CAUSING CONNECTION FLUSH

		s_staleConnErrors_priv.add(Integer.valueOf(28));
		// ORA-01003: no statement parsed
		s_staleConnErrors_priv.add(Integer.valueOf(1003));
		s_staleConnErrors_priv.add(Integer.valueOf(1012));
		s_staleConnErrors_priv.add(Integer.valueOf(1014));
		s_staleConnErrors_priv.add(Integer.valueOf(1033));
		s_staleConnErrors_priv.add(Integer.valueOf(1034));
		s_staleConnErrors_priv.add(Integer.valueOf(1035));
		s_staleConnErrors_priv.add(Integer.valueOf(1089));
		s_staleConnErrors_priv.add(Integer.valueOf(1090));
		s_staleConnErrors_priv.add(Integer.valueOf(1092));
		s_staleConnErrors_priv.add(Integer.valueOf(2068));
		s_staleConnErrors_priv.add(Integer.valueOf(3113));
		s_staleConnErrors_priv.add(Integer.valueOf(3114));
		s_staleConnErrors_priv.add(Integer.valueOf(12541));
		s_staleConnErrors_priv.add(Integer.valueOf(12560));
		s_staleConnErrors_priv.add(Integer.valueOf(12571));
		s_staleConnErrors_priv.add(Integer.valueOf(17002));
		s_staleConnErrors_priv.add(Integer.valueOf(17008));
		s_staleConnErrors_priv.add(Integer.valueOf(17009));
		s_staleConnErrors_priv.add(Integer.valueOf(17410));
		s_staleConnErrors_priv.add(Integer.valueOf(17401));
		s_staleConnErrors_priv.add(Integer.valueOf(25408));
		// ORA-17430: Must be logged on to server (TTC - Two-Task Common
		// Message)
		s_staleConnErrors_priv.add(Integer.valueOf(17430));
		// ORA-01475: must reparse cursor to change bind variable datatype
		s_staleConnErrors_priv.add(Integer.valueOf(1475));
		// ORA-23326: object group ABC.DEF is quiesced - added for Trinity
		s_staleConnErrors_priv.add(Integer.valueOf(23326));
		s_staleConnErrors_priv.add("Connection reset by peer");
		s_staleConnErrors_priv.add("55032");
		s_staleConnErrors_priv.add("08001");
		s_staleConnErrors_priv.add("08003");
		s_staleConnErrors_priv.add("40003");
		s_staleConnErrors_priv.add("S1000");
		s_staleConnErrors_priv.add("08S01");
		s_staleConnErrors_priv.add("08006");
		s_staleConnErrors_priv.add("OALL8 is in an inconsistent state.");
		s_staleConnErrors_priv.add("OCC error: OCC-100: backlog timeout");
		s_staleConnErrors_priv.add("OCC error: OCC-102: backlog eviction");
		s_staleConnErrors_priv.add("OCC error: OCC-103: request rejected, database down");
		s_staleConnErrors_priv.add("OCC error: OCC-104: saturation soft sql eviction");
		s_staleConnErrors_priv.add("Unexpected end of stream");
		s_staleConnErrors = Collections.unmodifiableSet(new HashSet<Object>(s_staleConnErrors_priv));

		// EXCEPTIONS COUNTING TOWARDS MARKDOWN
		// note that pool-flush exceptions are autumatically
		// counted towards markdown

		// ORA-01017: invalid username/password; logon denied
		s_markdownErrors_priv.add(Integer.valueOf(1017));
		// ORA-12535: TNS:operation timed out
		s_markdownErrors_priv.add(Integer.valueOf(12535));
		// ORA-12545: Connect failed because target host or object does not
		// exist
		s_markdownErrors_priv.add(Integer.valueOf(12545));
		s_markdownErrors = Collections.unmodifiableSet(new HashSet(s_markdownErrors_priv));

		// SERVER-SIDE EXCEPTIONS

		// ORA-01536: space quota exceeded for tablespace
		s_serverSideErrors_priv.add(Integer.valueOf(1536));
		// ORA-01552: cannot use system rollback segment
		// for non-system tablespace 'USERS'
		s_serverSideErrors_priv.add(Integer.valueOf(1552));
		s_serverSideErrors = Collections.unmodifiableSet(new HashSet(s_serverSideErrors_priv));

		// RESOURCE ALLOCATION EXCEPTIONS

		// ORA-00020: maximum number of processes (XXX) exceeded
		s_resourceAllocationErrors_priv.add(Integer.valueOf(20));
		// ORA-01000: maximum open cursors exceeded
		s_resourceAllocationErrors_priv.add(Integer.valueOf(1000));
		s_resourceAllocationErrors = Collections.unmodifiableSet(new HashSet(s_resourceAllocationErrors_priv));

		// BAD USER DATA EXCEPTIONS. LOG DO VALUES FOR THESE EXCEPTIONS

		// ORA-00001: unique constraint violation
		s_badUserDataErrors_priv.add(Integer.valueOf(1));
		// ORA-01401: inserted value too large for column
		s_badUserDataErrors_priv.add(Integer.valueOf(1401));
		// ORA-12899: value too large for column
		s_badUserDataErrors_priv.add(Integer.valueOf(12899));
		// ORA-01438: value larger than specified precision allows for
		// this column
		s_badUserDataErrors_priv.add(Integer.valueOf(1438));
		s_badUserDataErrors = Collections.unmodifiableSet(new HashSet(s_badUserDataErrors_priv));

		s_passwordWrongError_priv.add(Integer.valueOf(1017));
		s_passwordWrongError = Collections.unmodifiableSet(new HashSet(s_passwordWrongError_priv));
		

		// EXCEPTIONS IGNORED IN CAL LOGS

		// ORA-00001 Unique constraint violation
		s_ignorableErrors_priv.add(Integer.valueOf(1));
		// BUGDB00215854: LeaveFeedback - excessive CAL exceptions
		// ORA-00054: resource busy and acquire with NOWAIT specified
		s_ignorableErrors_priv.add(Integer.valueOf(54));
		s_ignorableErrors = Collections.unmodifiableSet(new HashSet(s_ignorableErrors_priv));

		// ORA-1031: adding write permissions ora error 1031 in
		// s_shouldReportAsMarkdownErrors
		s_shouldReportAsMarkdownErrors_priv.add(Integer.valueOf(1031));
		s_shouldReportAsMarkdownErrors = Collections.unmodifiableSet(new HashSet(s_shouldReportAsMarkdownErrors_priv));

		addMarkdownErrorWithSubstring(4031, "large pool");
	}

	private void addMarkdownErrorWithSubstring(int code, String text) {
		List<String> list = s_markdownErrorsWithSubstrings_priv.get(Integer.valueOf(code));
		if (list == null) {
			list = new ArrayList<String>();
		}
		list.add(text.toLowerCase());
		s_markdownErrorsWithSubstrings_priv.put(Integer.valueOf(code), list);

		s_markdownErrorsWithSubstrings = Collections.unmodifiableMap(new HashMap<Integer, List<String>>(s_markdownErrorsWithSubstrings_priv));
	}
	
	public boolean shouldLogInCal(SQLException sqle) {
		for (SQLException e = sqle; e != null; e = e.getNextException()) {
			int errorCode = e.getErrorCode();

			if (errorCode >= CUSTOM_ERR_MIN_CODE &&
				errorCode <= CUSTOM_ERR_MAX_CODE)
			{
				// this is a custom PL/SQL error
				return false;
			}

			if (s_ignorableErrors.contains(Integer.valueOf(errorCode))) {
				// this is one of the errors we never report
				return false;
			}
		}

		return true;
	}

}
