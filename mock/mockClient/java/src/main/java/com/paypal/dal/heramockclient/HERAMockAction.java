package com.paypal.dal.heramockclient;

import com.paypal.dal.heramockclient.mockannotation.JDBCMockConst;

public class HERAMockAction {
    public static final String FAIL_ON_COMMIT = "FAIL_ON_COMMIT";
    public static final String FAIL_ON_ROLLBACK = "FAIL_ON_ROLLBACK";
    public static final String FAIL_SET_SHAREDID = "FAIL_SET_SHAREDID";
    public static final String TIMEOUT_ON_COMMIT = "TIMEOUT_ON_COMMIT";
    public static final String DELAY_ON_COMMIT = " DELAY_ON_COMMIT";
    public static final String TIMEOUT_ON_ROLLBACK = "TIMEOUT_ON_ROLLBACK";
    public static final String FAIL_ON_PING = "FAIL_ON_PING";
    public static final String TIMEOUT = "timeout";

    public static final String RESPOND_WITH_INCONSISTENT_STATE = "OALL8 is in an inconsistent state.";

    public static final String RESPOND_WITH_ORA00904 = "1 904 ORA-00904: \"A\".\"ACCOUNT_NUMBED\": invalid identifier";
    public static final String RESPOND_WITH_BACKLOG = "2 HERA-100: backlog timeout";
    public static final String RESPOND_WITH_BACKLOG_EVICTION = "2 HERA-102: backlog eviction";
    public static final String RESPOND_WITH_SATURATION_KILL = "2 HERA-101: saturation kill";
    public static final String RESPOND_DATABASE_DOWN = "2 HERA-103: request rejected, database down";
    public static final String RESPOND_WITH_SATURATION_SOFT_EVICTION = "2 HERA-104: saturation soft sql eviction";
    public static final String RESPOND_WITH_BIND_THROTTLE_ERROR = "2 HERA-105: bind throttle";
    public static final String RESPOND_WITH_BIND_EVICTION_ERROR = "2 HERA-106: bind eviction";
    public static final String RESPOND_WITH_HERA_BIND_THROTTLE_ERROR = "2 HERA-105: bind throttle";
    public static final String RESPOND_WITH_HERA_BIND_EVICTION_ERROR = "2 HERA-106: bind eviction";

    public static final String RESPOND_WITH_MARKDOWN = "8 testing mark down";
    public static final String PROTOCOL_ERROR_ON_ROLLBACK = "PROTOCOL_ERROR_ON_ROLLBACK";
    public static final String PROTOCOL_ERROR_ON_COMMIT = "PROTOCOL_ERROR_ON_COMMIT";
    public static final String PROTOCOL_EXTRA_DATA_ON_ROLLBACK = "PROTOCOL_EXTRA_DATA_ON_ROLLBACK";
    public static final String NOMOCK = "NOMOCK";
    public static final String ADD_MOCK_CONSTRAINT = " MKEYSEP ";

    static final String ACCEPT = "accept";
    public final static String CONNECTION_TIMEOUT = "connect_timeout";
    public final static String RESPONSE_TIMEOUT = "response_timeout";
    public final static String SIMULATE_AUTH_FAILURE = "1005 simulating auth failure";
    public final static String SIMULATE_CLOCK_SKEW = "1010 simulating clockskew";
    public final static String CAPTURE = "CAPTURE";
    public final static String REPLAY = "REPLAY";
    public final static String NEXT = " NEXT_COMMAND_REPLY ";
    public final static String NEXT_QUERY = JDBCMockConst.NEW_LINE + "NEXT_COMMAND_REPLY";
    public final static String MOCK_SERVER_DETAIL = "5 stage2mock_hera:load_saved_sessions*CalThreadId=0*" +
            "TopLevelTxnStartTime=TopLevelTxn not set*Host=mockhost,";

    public final static String PING_PONG_REPLY = " 1009,";
    public final static String OK = " 5,";
    public final static String CLOSE_SOCKET = " CLOSE_SOCKET";
    public final static String INSERT_SUCCESS = "0 3:3 0,3:3 1,,";
    public final static String AUTO_COMMIT_RESP = "5,";
    public final static String INSERT_FAILURE = "1 904 ORA-00904: Dummy simulated error,,";
    public final static String TIMEOUT_ON_FETCH = "TIMEOUT_ON_FETCH";
}
