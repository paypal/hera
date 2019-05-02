package com.paypal.hera.constants;

public class OccConstants {
	// server commands infra/utility/core/net/ServerCommands.h
	public static final int SERVER_CHALLENGE = 1001;
	public static final int SERVER_CONNECTION_ACCEPTED = 1002;
	public static final int SERVER_CONNECTION_REJECTED_PROTOCOL = 1003;
	public static final int SERVER_CONNECTION_REJECTED_UNKNOWN_USER = 1004;
	public static final int SERVER_CONNECTION_REJECTED_FAILED_AUTH = 1005;
	public static final int SERVER_UNEXPECTED_COMMAND = 1006;
	public static final int SERVER_INTERNAL_ERROR = 1007;
	public static final int SERVER_ALIVE = 1009;
	public static final int SERVER_CONNECTION_REJECTED_CLIENT_TIME = 1010;
	public static final int SERVER_INFO = 1011;
	
	public static final byte[] CLIENT_PROTOCOL_NAME_NOAUTH = "2001".getBytes();
	public static final byte[] CLIENT_PROTOCOL_NAME = "2002".getBytes();
	public static final byte[] CLIENT_USERNAME = "2003".getBytes();
	public static final byte[] CLIENT_CHALLENGE_RESPONSE = "2004".getBytes();
	public static final byte[] CLIENT_CURRENT_CLIENT_TIME = "2005".getBytes();
	public static final byte[] CLIENT_CAL_CORRELATION_ID = "2006".getBytes();
	public static final byte[] SERVER_PING_COMMAND = "1008".getBytes();
	
	// OCC commands
	public static final byte[] OCC_PREPARE       = "1".getBytes();
	public static final byte[] OCC_BIND_NAME     ="2".getBytes();
	public static final byte[] OCC_BIND_VALUE    ="3".getBytes();
	public static final byte[] OCC_EXECUTE       ="4".getBytes();;
	public static final byte[] OCC_ROWS          ="5".getBytes();;
	public static final byte[] OCC_COLS          ="6".getBytes();;
	public static final byte[] OCC_FETCH         ="7".getBytes();;
	public static final byte[] OCC_COMMIT        ="8".getBytes();;
	public static final byte[] OCC_ROLLBACK      ="9".getBytes();;
	public static final byte[] OCC_BIND_TYPE     ="10".getBytes();;
	public static final byte[] OCC_CLIENT_INFO   ="11".getBytes();;
	public static final byte[] OCC_BACKTRACE     ="12".getBytes();;
	public static final byte[] OCC_BIND_OUT_NAME ="13".getBytes();;
	public static final byte[] OCC_PREPARE_SPECIAL ="14".getBytes();;
	public static final byte[] OCC_TRANS_START     ="15".getBytes();;
	public static final byte[] OCC_TRANS_TIMEOUT	="16".getBytes();;
	public static final byte[] OCC_TRANS_ROLE		="17".getBytes();;
	public static final byte[] OCC_TRANS_PREPARE	="18".getBytes();;
	public static final byte[] OCC_SQL_STMT_CACHING	="19".getBytes();;
	public static final byte[] OCC_COLS_INFO		="22".getBytes();;
	public static final byte[] OCC_PREPARE_V2		="25".getBytes();;
	public static final byte[] OCC_ARRAY_LENGTH		="23".getBytes();;
	public static final byte[] OCC_ARRAY_MAX_VALUESZ="24".getBytes();;
	public static final byte[] OCC_SHARD_KEY		="27".getBytes();;
	public static final byte[] OCC_GET_NUM_SHARDS	="28".getBytes();;
	public static final byte[] OCC_SET_SHARD_ID		="29".getBytes();;

	// return codes
	public static final int OCC_SQL_ERROR = 1;
	public static final int OCC_ERROR = 2;
	public static final int OCC_VALUE = 3;
	public static final int OCC_HELLO = 4;
	public static final int OCC_OK = 5;
	public static final int OCC_NO_MORE_DATA = 6;
	public static final int OCC_STILL_EXECUTING = 7;
	public static final int OCC_MARKDOWN = 8;
	
	public static final byte[] CLIENT_PROTOCOL_NAME_VALUE = "occ 1".getBytes();
	public static final int PROTOCOL_VERSION = 1;
	
	public static final int MAX_SCUTTLE_BUCKETS = 1024;
	
}
