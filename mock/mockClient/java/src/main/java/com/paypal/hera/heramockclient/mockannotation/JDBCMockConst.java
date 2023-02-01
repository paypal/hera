package com.paypal.hera.heramockclient.mockannotation;

import com.paypal.hera.heramockclient.HERAMockHelper;

import java.util.HashMap;
import java.util.Map;

public class JDBCMockConst {
    public static final String CTRL_CMD = "0 ";
    public static final String VAL = "3 ";
    public static final String SEP = ":";
    public static final String NEXT = ",";
    public static final String ZERO_ROWS = "3:3 0,";
    public static final String NEW_LINE = " NEXT_NEWSTRING ";
    public static final String RESPONSE = " START_RESPONSE ";
    public static final String REQUEST = "NEW_REQUEST ";
    public static final String REPLAY_REQ = " HERAMOCK_NEW_SOCK ";
    public static final String MOCK_COMMIT_FOREVER = "MOCK_COMMIT_FOREVER";
    public static final String MOCK_ROLLBACK_FOREVER = "MOCK_ROLLBACK_FOREVER";
    public static final String MOCK_DELAYED_RESPONSE = " MOCK_DELAYED_RESPONSE ";
    public static final String DISABLE_LOG = "DISABLE_LOG";
    public static final String LOAD_BASED_MOCK = "LOAD_BASED_MOCK ";
    public static final String HERAMOCK_TABLESEP = " HERAMOCK_TABLESEP ";
    public static final String FOREVER = "FOREVER NEXT_COMMAND_REPLY ";

    static Map<String, String> cmdToTxt = new HashMap<String, String>() {
        {
            put("6,", "NO_MORE_DATA");
            put("5,", "OK");
            put("8,", "COMMIT");
            put("9,", "ROLLBACK");
        }
    };

    static Map<String, String> textToCmd = new HashMap<String, String>() {
        {
            put("NO_MORE_DATA", "6,");
            put("OK", "5,");
            put("COMMIT", "8,");
            put("ROLLBACK", "9,");
        }
    };

    public static String getCmd(String text){
        if (textToCmd.containsKey(text) && HERAMockHelper.getDecode())
            return textToCmd.get(text);
        return text;
    }

    public static String getText(String cmd){
        if (cmdToTxt.containsKey(cmd) && HERAMockHelper.getDecode())
            return cmdToTxt.get(cmd);
        return cmd;
    }
}
