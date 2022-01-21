package com.paypal.dal.heramockclient;

import java.util.HashMap;
import java.util.Map;

public class HERADataTypes {
        static Map<Integer, String> typeMap = new HashMap<Integer, String>();

        static Map<String, Integer> reverseTypeMap = new HashMap<String, Integer>();
        // ocidfn.h
        static {
            typeMap.put(1 /*SQLT_CHR*/, "VARCHAR_1");
            typeMap.put(2 /*SQLT_NUM*/, "NUMERIC_2");
            typeMap.put(3 /*SQLT_INT*/, "INTEGER_3");
            typeMap.put(4 /*SQLT_FLT*/, "FLOAT_4");
            typeMap.put(5 /*SQLT_STR*/, "VARCHAR_5");
            typeMap.put(6 /*SQLT_VNU NUM with preceding length byte*/, "VARCHAR_6");
            typeMap.put(7 /*SQLT_PDN*/, "VARCHAR_7");
            typeMap.put(8 /*SQLT_LNG*/, "BIGINT_8");
            typeMap.put(9 /*SQLT_VCS*/, "VARCHAR_9");
            typeMap.put(12 /*SQLT_DAT*/, "DATE_12");
            typeMap.put(15 /*SQLT_VBI*/, "VARBINARY_15");
            typeMap.put(21 /*SQLT_BFLOAT*/, "FLOAT_21");
            typeMap.put(22 /*SQLT_BDOUBLE*/, "DOUBLE_22");
            typeMap.put(23 /*SQLT_BIN*/, "VARBINARY_23");
            typeMap.put(24 /*SQLT_LBI*/, "LONGVARBINARY_24");
            typeMap.put(68 /*SQLT_UIN*/, "NUMERIC_68");
            typeMap.put(94 /*SQLT_LVC*/, "LONGVARCHAR_94");
            typeMap.put(95 /*SQLT_LVB*/, "LONGVARBINARY_95");
            typeMap.put(96 /*SQLT_AFC*/, "VARCHAR_96");
            typeMap.put(97 /*SQLT_AVC*/, "VARCHAR_97");
            typeMap.put(104 /*SQLT_RDD - rowid type*/, "VARCHAR_104");
            typeMap.put(112 /*SQLT_CLOB*/, "CLOB_112");
            typeMap.put(113 /*SQLT_BLOB*/, "BLOB_113");
            typeMap.put(155 /*SQLT_VST*/, "VARCHAR_155");
            typeMap.put(156 /*SQLT_ODT*/, "DATE_156");
            typeMap.put(184 /*SQLT_DATE*/, "DATE_184");
            typeMap.put(185 /*SQLT_TIME*/, "TIME_185");
            typeMap.put(186 /*SQLT_TIME_TZ*/, "TIME_186");
            typeMap.put(187 /*SQLT_TIMESTAMP*/, "TIMESTAMP_187");
            typeMap.put(188 /*SQLT_TIMESTAMP_TZ*/, "TIMESTAMP_188");
            typeMap.put(232 /*SQLT_TIMESTAMP_LTZ*/, "TIMESTAMP_232");

            for(Integer k : typeMap.keySet()) {
                reverseTypeMap.put(typeMap.get(k), k);
            }
        }
}
