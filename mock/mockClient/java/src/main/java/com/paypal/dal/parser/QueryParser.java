package com.paypal.dal.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paypal.dal.parser.sqlmetadata.ColumnDetails;
import com.paypal.dal.parser.sqlmetadata.InsertItemMetaData;
import com.paypal.dal.parser.sqlmetadata.SelectItemMetaData;
import com.paypal.dal.parser.sqlmetadata.WhereBindInMeta;
import net.sf.jsqlparser.statement.select.Select;

import java.util.HashMap;
import java.util.Map;

public class QueryParser {
    public static void main(String[] parameters) {
        if (parameters[0].equals("QUERY_META")) {
            System.out.println(getMockMetaForQuery(parameters[1], parameters[2]));
        } else if (parameters[0].equals("QUERY_RESPONSE")) {
            System.out.println(getMockRowsForQuery(parameters[1], parameters[2], false));
        } else {
            System.out.println("unknown command " + parameters[0]);
        }
//        System.out.println(new JSONObject(json).toString(4));
    }

    public static String getMockRowsForQuery(String sql, String mockData, boolean alongWithMeta){
        Parser parser = new Parser();
        StringBuilder response = new StringBuilder();
        boolean isMeta = true;
        int counter = 0;
        try {
            parser.parse(sql);
            if (parser.sqlMetaData.getSqlType().equals(Select.class.getSimpleName())) {
                for (String values : mockData.split("NEXT_NEWSTRING ")) {
                    if(values.trim().startsWith(",") || values.trim().length() == 0 || values.trim().equals("NEXT_NEWSTRING"))
                        continue;
                    if(isMeta) {
                        isMeta = false;
                        continue;
                    }
                    if (response.length() > 0)
                        response.append(", NEXT_NEWSTRING 0 ");
                    else
                        response.append("0 ");
                    for (SelectItemMetaData itemMetaData : parser.sqlMetaData.getSelectMetaData().getSelectItemMetaDataList()) {
                        if (itemMetaData.getSelectLevel() == 1) {
                            String startKey = itemMetaData.getColumnName().toUpperCase() + "_START_HERA_MOCK ";
                            String endKey = " " + itemMetaData.getColumnName().toUpperCase() + "_END_HERA_MOCK ";
                            if (values.contains(startKey) && values.contains(endKey)) {
                                String value = values.split(startKey)[1].split(endKey)[0];
                                String temp = "3";
                                if (value.length() > 0)
                                    temp += " " + value;
                                response.append(temp.length()).append(":").append(temp).append(",");
                                counter++;
                            } else if (itemMetaData.getColumnName().equals("*")) {
                                for (String token : mockData.split("NEXT_NEWSTRING ")[0].split(" ")) {
                                    if (token.endsWith("_START_HERA_MOCK")) {
                                        String endToken = token.split("_START_HERA_MOCK")[0] + "_END_HERA_MOCK";
                                        String temp = "3";
                                        if (values.contains(token + " ") && values.contains(" " + endToken)) {
                                            String value = values.split(token + " ")[1].split(" " + endToken)[0];
                                            if (value.length() > 0)
                                                temp += " " + value;
                                        }
                                        response.append(temp.length()).append(":").append(temp).append(",");
                                        counter++;
                                    }
                                }
                            } else {
                                response.append("1:3,");
                                counter++;
                            }
                        }
                    }
                }
            }
        }catch (Exception e) {
            return e.getMessage();
        }

        if(!alongWithMeta) {
            String firstLine = "0";
            String temp = "3 " + counter;
            firstLine += " " + temp.length() + ":" + temp + ",3:3 0,, NEXT_NEWSTRING ";
            return firstLine + response + ", NEXT_NEWSTRING 6,";
        }

        return response + ", NEXT_NEWSTRING 6,";
    }

    public static String getMockMetaForQuery(String sql, String metaData) {
        Parser parser = new Parser();
        StringBuilder response = new StringBuilder();
        String header = "0";
        int counter = 0;
        try {
            parser.parse(sql);
            if (parser.sqlMetaData.getSqlType().equals(Select.class.getSimpleName())) {
                for (SelectItemMetaData itemMetaData : parser.sqlMetaData.getSelectMetaData().getSelectItemMetaDataList()) {
                    if (itemMetaData.getSelectLevel() == 1) {
                        String startKey = itemMetaData.getColumnName().toUpperCase() + "_START_HERA_MOCK ";
                        String endKey = itemMetaData.getColumnName().toUpperCase() + "_END_HERA_MOCK ";
                        if(metaData.contains(startKey) && metaData.contains(endKey)) {
                            response.append(metaData.split(startKey)[1].split(endKey)[0]);
                            counter++;
                        } else if (itemMetaData.getColumnName().equals("*"))
                        {
                            for(String token : metaData.split("NEXT_NEWSTRING ")[0].split(" ")){
                                if (token.endsWith("_START_HERA_MOCK")) {
                                    String endToken = token.split("_START_HERA_MOCK")[0] + "_END_HERA_MOCK";
                                    response.append(metaData.split(token + " ")[1].split(endToken + " ")[0]);
                                    counter++;
                                }
                            }
                        }
                        else if (startKey.equals("NEXTVAL_START_HERA_MOCK ")) {
                            startKey = "NEXT_VAL_START_HERA_MOCK ";
                            endKey = "NEXT_VAL_END_HERA_MOCK ";
                            if(metaData.contains(startKey) && metaData.contains(endKey)) {
                                response.append(metaData.split(startKey)[1].split(endKey)[0]);
                                counter++;
                            }
                        }
                        else{
                            return "Missing MetaData for " + itemMetaData.getColumnName();
                        }
                    }
                }
            }
        }catch (Exception e)
        {
            return e.getMessage();
        }
        String firstLine = "0";
        String temp = "3 " + counter;
        firstLine += " " + temp.length() + ":" + temp + ",3:3 0,, NEXT_NEWSTRING ";
        header += " " + temp.length() + ":" + temp + ",";
        return firstLine + header + response + ", NEXT_NEWSTRING " + getMockRowsForQuery(sql, metaData, true);
    }

    public static String process(String inputSql)
    {
        Map<String, Object> elements = new HashMap();
        ObjectMapper objectMapper = new ObjectMapper();
        Parser parser = new Parser();
        try {
            parser.parse(inputSql);
            elements.put("sql", inputSql);
            elements.put("type", parser.sqlMetaData.getSqlType());
            elements.put("tables", parser.sqlMetaData.getTableMetaData());

            Map<String, Object> binds = new HashMap();
            Map<String, String> columnAlias = new HashMap();

            for(ColumnDetails columnDetails : parser.sqlMetaData.getColumnMetaData().getColumnDetails()) {
                String value = columnDetails.getColumnName();
                if(columnDetails.getTableName() != null)
                    value = columnDetails.getTableName() + "." + columnDetails.getColumnName();
                columnAlias.put(columnDetails.getAliasName(), value);
            }

            for (WhereBindInMeta whereBindInMeta : parser.sqlMetaData.getSelectMetaData().getWhereBindInMetaList()) {
                String key = "";
                String value = "";
                if (whereBindInMeta.getVariableName().size() > 0) {
                    if(whereBindInMeta.getTableName() != null) {
                        key = whereBindInMeta.getTableName() + ".";
                    }
                    if(whereBindInMeta.getColumnName() != null) {
                        if (columnAlias.containsKey(whereBindInMeta.getColumnName()))
                            key += columnAlias.get(whereBindInMeta.getColumnName());
                        else
                            key += whereBindInMeta.getColumnName();
                    } else if (whereBindInMeta.getConstantValue() != null) {
                        key = whereBindInMeta.getConstantValue().getType() + "." +
                                whereBindInMeta.getConstantValue().getValue();
                    }
                    if(whereBindInMeta.getVariableName().size() > 2) {
                        value = whereBindInMeta.getVariableName().subList(0, 2).toString() + " (" +
                                whereBindInMeta.getVariableName().size() + " more)";
                    } else {
                        value = whereBindInMeta.getVariableName().toString();
                    }
                    if(binds.containsKey(key)) {
                        key = key + "_" + binds.keySet().size();
                    }
                    binds.put(key, value);
                }
            }

            int count = 1;
            for(InsertItemMetaData insertItemMetaData :
                    parser.sqlMetaData.getInsertMetaData().getInsertItemMetaDataList()) {
                if (insertItemMetaData.getVariableName() != null) {
                    String key = insertItemMetaData.getColumnName();
                    if(insertItemMetaData.getTableName() != null) {
                        key = insertItemMetaData.getTableName() + "." + insertItemMetaData.getColumnName();
                    }
                    if (key == null) {
                        key = String.valueOf(count);
                        count++;
                    }
                    binds.put(key, insertItemMetaData.getVariableName());
                }
            }

            Map<String, Object> updateColumn = new HashMap<>();
            for(InsertItemMetaData insertItemMetaData :
                    parser.sqlMetaData.getUpdateMetaData().getUpdateItemMetaDataList()) {
                if (insertItemMetaData.getVariableName() != null) {
                    String key = insertItemMetaData.getColumnName();
                    if(insertItemMetaData.getTableName() != null) {
                        key = insertItemMetaData.getTableName() + "." + insertItemMetaData.getColumnName();
                    }
                    updateColumn.put(key, insertItemMetaData.getVariableName());
                }
            }
            if(updateColumn.size() > 0)
                binds.put("UpdateBind", updateColumn);

            Map<String, Object> updateWhereBind = new HashMap<>();
            for(WhereBindInMeta whereBindInMeta :
                    parser.sqlMetaData.getUpdateMetaData().getWhereBindInMetaList()) {
                if (whereBindInMeta.getVariableName() != null) {
                    String key = whereBindInMeta.getColumnName();
                    if(whereBindInMeta.getTableName() != null) {
                        key = whereBindInMeta.getTableName() + "." + whereBindInMeta.getColumnName();
                    }
                    if (key == null) {
                        key = String.valueOf(count);
                        count++;
                    }
                    updateWhereBind.put(key, whereBindInMeta.getVariableName());
                }
            }
            if(updateWhereBind.size() > 0)
                binds.put("UpdateWhereBind", updateWhereBind);

            elements.put("bindIn", binds);
            elements.put("status", "success");
            String json = objectMapper.writeValueAsString(elements);
            return json;

        }catch (Exception|Error e) {
            elements.put("status", "exception");
            String msg = "Failed for " + inputSql + " with Error message " + e.getMessage();
            elements.put("msg", msg);
            try {
                String json = objectMapper.writeValueAsString(elements);
                return json;
            } catch (Exception ex){
                return "{\"status\":\"exception\",\"msg\":\"failed while converting error msg: " +
                        ex.getMessage() + "\"}";
            }
        }
    }
}
