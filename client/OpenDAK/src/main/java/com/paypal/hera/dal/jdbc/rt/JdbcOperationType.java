package com.paypal.hera.dal.jdbc.rt;

/**
 * Defines type of database operation intercepted by CM Proxy
 * 
 */
public final class JdbcOperationType {

	private static final long serialVersionUID = -3454117283389670364L;
	public static final JdbcOperationType CONN_MISC =
		new JdbcOperationType("CONN_MISC", 100, false);
	public static final JdbcOperationType CONN_CREATE_STMT =
		new JdbcOperationType("CONN_CREATE_STMT", 101, false);
	public static final JdbcOperationType CONN_PREP_STMT =
		new JdbcOperationType("CONN_PREP_STMT", 102, false);
	public static final JdbcOperationType CONN_PREP_CALL =
		new JdbcOperationType("CONN_PREP_CALL", 103, false);
	public static final JdbcOperationType CONN_COMMIT =
		new JdbcOperationType("CONN_COMMIT", 104, false);
	public static final JdbcOperationType CONN_ROLLBACK =
		new JdbcOperationType("CONN_ROLLBACK", 105, false);
	public static final JdbcOperationType CONN_CREATE =
		new JdbcOperationType("CONN_CREATE", 106, false);

	public static final JdbcOperationType STMT_EXEC =
		new JdbcOperationType("STMT_EXEC", 200, true);
	public static final JdbcOperationType STMT_EXEC_QUERY =
		new JdbcOperationType("STMT_EXEC_QUERY", 201, false);
	public static final JdbcOperationType STMT_EXEC_UPDATE =
		new JdbcOperationType("STMT_EXEC_UPDATE", 202, true);
	public static final JdbcOperationType STMT_CLOSE =
		new JdbcOperationType("STMT_CLOSE", 203, false);
	public static final JdbcOperationType STMT_CANCEL =
		new JdbcOperationType("STMT_CANCEL", 204, false);
	public static final JdbcOperationType STMT_GET_RESULTSET =
		new JdbcOperationType("STMT_GET_RESULTSET", 205, false);

	public static final JdbcOperationType PREP_STMT_EXEC =
		new JdbcOperationType("PREP_STMT_EXEC", 300, true);
	public static final JdbcOperationType PREP_STMT_EXEC_QUERY =
		new JdbcOperationType("PREP_STMT_EXEC_QUERY", 301, false);
	public static final JdbcOperationType PREP_STMT_EXEC_UPDATE =
		new JdbcOperationType("PREP_STMT_EXEC_UPDATE", 302, true);
	public static final JdbcOperationType PREP_STMT_EXEC_BATCH =
		new JdbcOperationType("PREP_STMT_EXEC_BATCH", 303, true);
	public static final JdbcOperationType PREP_STMT_SET_PARAM =
		new JdbcOperationType("PREP_STMT_SET_PARAM", 304, false);

	public static final JdbcOperationType RS_CLOSE =
		new JdbcOperationType("RS_CLOSE", 400, false);
	public static final JdbcOperationType RS_NEXT =
		new JdbcOperationType("RS_NEXT", 401, false);
	public static final JdbcOperationType RS_POS_CHANGE =
		new JdbcOperationType("RS_POS_CHANGE", 402, false);
	public static final JdbcOperationType RS_UPDATE =
		new JdbcOperationType("RS_UPDATE", 403, true);

	public static final JdbcOperationType LOB_POSITION =
		new JdbcOperationType("LOB_POSITION", 501, false);
	public static final JdbcOperationType LOB_GET_STREAM =
		new JdbcOperationType("LOB_GET_STREAM", 502, false);
	public static final JdbcOperationType LOB_GET_DATA =
		new JdbcOperationType("LOB_GET_DATA", 503, false);
	public static final JdbcOperationType LOB_SET_STREAM =
		new JdbcOperationType("LOB_SET_STREAM", 504, false);
	public static final JdbcOperationType LOB_SET_DATA =
		new JdbcOperationType("LOB_SET_DATA", 505, false);
		
	private final boolean m_isDmlOperation;

/*	protected Object readResolve() throws ObjectStreamException {
		return super.readResolve();
	}
*/

	private JdbcOperationType(String name, int intValue,
		boolean isDmlOperation)
	{
		//super(intValue, name);
		m_isDmlOperation = isDmlOperation;
	}   

//	public static JdbcOperationType get(int key) {
//		return (JdbcOperationType)getEnum(
//			JdkUtil.forceInit(JdbcOperationType.class), key);
//	}   
//
//	public static JdbcOperationType getElseReturn(
//		int key, JdbcOperationType elseEnum)
//	{  
//		return (JdbcOperationType)getElseReturnEnum(
//			JdkUtil.forceInit(JdbcOperationType.class), key, elseEnum);
//	}   
//
//	public static ListIterator iterator() {
//		return getIterator(JdkUtil.forceInit(JdbcOperationType.class));
//	}                         

	/**
	 * Checks whether operation could potentially modify the data
	 */
	public boolean isDmlOperation() {
		return m_isDmlOperation;
	}
}
