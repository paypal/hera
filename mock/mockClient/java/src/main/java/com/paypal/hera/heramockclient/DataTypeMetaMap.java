package com.paypal.hera.heramockclient;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class DataTypeMetaMap {
    private static String NUMBER = "3:3 2,3:3 0,3:3 0,5:3 129,";
    private static String STRING = "3:3 1,3:3 8,3:3 0,3:3 0,";
    private static String INTEGER = "3:3 3,3:3 0,3:3 0,3:3 0,";
    private static String TIME = "5:3 185,3:3 0,3:3 0,3:3 0,";

    static private Map<String, String> metaMap = new HashMap<>();
    static private DataTypeMetaMap dataTypeMetaMap = new DataTypeMetaMap();



    private DataTypeMetaMap() {
        metaMap.put(int.class.getSimpleName(), NUMBER);
        metaMap.put(BigDecimal.class.getSimpleName(), NUMBER);
        metaMap.put(BigInteger.class.getSimpleName(), NUMBER);
        metaMap.put(String.class.getSimpleName(), STRING);
        metaMap.put(long.class.getSimpleName(), NUMBER);
        metaMap.put(Long.class.getSimpleName(), NUMBER);
        metaMap.put(double.class.getSimpleName(), NUMBER);
        metaMap.put(Double.class.getSimpleName(), NUMBER);
        metaMap.put(Double.class.getSimpleName(), NUMBER);
        metaMap.put(Integer.class.getSimpleName(), INTEGER);
        metaMap.put(Timestamp.class.getSimpleName(), TIME);
    }

    public DataTypeMetaMap getDataTypeMetaMap(){
        return dataTypeMetaMap;
    }

    public static String variableCaseToBufferCase(String input){
        if (input.startsWith("m_"))
            input = input.substring(2);
        StringBuilder output = new StringBuilder();
        for(char ch : input.toCharArray()) {
            if (Character.isUpperCase(ch) && !input.equals("nextVal"))
                output.append("_");
            output.append(ch);
        }
        return output.toString().toUpperCase();
    }

    public static String bufferCaseToVariableCase(String input){
        StringBuilder output = new StringBuilder();
        boolean nextUpper = false;
        for(char ch : input.toLowerCase(Locale.ROOT).toCharArray()) {
            if (ch == '_') {
                nextUpper = true;
                continue;
            }
            if (nextUpper)
                output.append( Character.toUpperCase(ch));
            else
                output.append(ch);
            nextUpper = false;
        }
        return output.toString();
    }
    public static String getEquivalent(String name, String dataType) throws HERAMockException {
        String bufferCase = variableCaseToBufferCase(name);
        String temp = "3 " + bufferCase;
        String fieldStart = bufferCase + "_START_HERA_MOCK ";
        String fieldEnd = bufferCase + "_END_HERA_MOCK ";
        String msg = temp.length() + ":" + temp + ",";
        if (!metaMap.containsKey(dataType)) {
            throw new HERAMockException("Data Type not defined yet - please contact DAL team to add it");
        }
        return fieldStart + msg + metaMap.get(dataType) + fieldEnd;
    }
}
