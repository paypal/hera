package com.paypal.dal.heramockclient;

import com.paypal.hera.util.NetStringObj;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class NSReader {

    Map<Long, String> commandMap = new HashMap();
    Map<String, String> reverseCommandMap = new HashMap();
    Map<String, String> rawCommandMap = new HashMap();
    Map<String, String> reverseRawCommandMap = new HashMap();


    static boolean requestedColInfo(JSONObject e, boolean decode) {
        if(decode){
            return e.getJSONObject("request").has("getColumn");
        }
        return e.getString("request").contains(",1:4,2:22,");
    }

    NSReader() {
        commandMap.put(25l, "query");
        commandMap.put(22l, "getColumn");
        commandMap.put(7l, "fetch");

        rawCommandMap.put("8,", "commit");
        rawCommandMap.put("9,", "rollback");
        rawCommandMap.put("6,", "noMoreData");

        for(Long key : commandMap.keySet()) {
            String val = String.valueOf(key);
            reverseCommandMap.put(commandMap.get(key), val);
        }

        for(String key : rawCommandMap.keySet()) {
            reverseRawCommandMap.put(rawCommandMap.get(key), key);
        }
    }

    Iterator<NetStringObj> parse(String data) throws IOException {
        ArrayList<NetStringObj> responses = new ArrayList<NetStringObj>();
        NetStringObj netStringObj = new NetStringObj(new ByteArrayInputStream(data.getBytes()));
        if (netStringObj.getCommand() == 0) {
            ByteArrayInputStream bais = new ByteArrayInputStream(netStringObj.getData());
            while (bais.available() > 0) {
                NetStringObj nso = new NetStringObj(bais);
                responses.add(nso);
            }
        } else {
            responses.add(netStringObj);
        }
        return responses.iterator();
    }

    private String addLength(String inp) {
        return inp.length() + ":" + inp + ",";
    }

    private void reverseColumns(JSONObject responseObject, int numberOfColumns,
                                JSONArray response, String query) {
        if (responseObject.has("columns")) {
            JSONArray columns = responseObject.getJSONArray("columns");
            StringBuilder resp = new StringBuilder();
            resp.append("0 ");
            String temp = "3 " + numberOfColumns;
            resp.append(addLength(temp));
            boolean columnCacheFound = ColumnCache.cache.size() > 0;
            List<Column> columnDetails = new ArrayList<Column>();
            for (int i = 0; i < columns.length(); i++) {
                JSONObject object = columns.getJSONObject(i);
                String columnName = object.getString("ColumnName");
                String type = object.getString("type");
                String width = object.getString("width");
                String precision = object.getString("precision");
                String scale = object.getString("scale");

                if(!columnCacheFound) {
                    Column column = new Column();
                    column.setName(columnName);
                    column.setType(type);
                    column.setWidth(width);
                    column.setPrecision(precision);
                    column.setScale(scale);
                    column.setIndex(i);
                    columnDetails.add(column);
                }

                temp = "3 " + columnName;
                resp.append(addLength(temp));
                temp = "3 " + HERADataTypes.reverseTypeMap.get(type);
                resp.append(addLength(temp));
                temp = "3 " + width;
                resp.append(addLength(temp));
                temp = "3 " + precision;
                resp.append(addLength(temp));
                temp = "3 " + scale;
                resp.append(addLength(temp));
            }
            response.put(resp.toString() + ",");
            if(!columnCacheFound) {
                ColumnCache.cache.put(query, columnDetails);
            }
        }
    }

    JSONArray reverseResponse(JSONObject responseObject,
                              JSONObject requestObject) {
        JSONArray response = new JSONArray();
        int numberOfColumns = -1;
        String query = "";
        if (requestObject.has("query"))
            query = requestObject.getString("query");
        if(responseObject.has("rows") && responseObject.getJSONArray("rows").length() >0) {
            JSONArray array = responseObject.getJSONArray("rows");
            if (array.length() > 0 && query.length() > 0) {
                numberOfColumns = array.getJSONObject(0).keySet().size();
                String resp = new String();
                String temp = "3 " + numberOfColumns;
                resp += "0 " + temp.length() + ":" + temp;
                resp += ",3:3 0,,";
                response.put(resp);
            }

            reverseColumns(responseObject, numberOfColumns, response, query);

            JSONArray rows = responseObject.getJSONArray("rows");
            StringBuilder resp = new StringBuilder();
            resp.append("0 ");
            for (int i = 0; i < rows.length(); i++) {
                JSONObject object = rows.getJSONObject(i);
                List<Column> columns = ColumnCache.cache.get(query);
                for(int j=0; j<numberOfColumns; j++) {
                    String temp = "3";
                    if(object.getString(columns.get(j).getName()).length() > 0)
                        temp += " " + object.getString(columns.get(j).getName());
                    resp.append(addLength(temp));
                }
            }
            response.put(resp.toString() + ",");
            response.put("6,");

        } else if (responseObject.has("recordsAffected")){
            JSONObject recordsAffected = responseObject.getJSONObject("recordsAffected");
            int cols = recordsAffected.getInt("columns");
            int rows = recordsAffected.getInt("rows");
            String resp = new String();
            String temp = "3 " + cols;
            resp += "0 " + addLength(temp);
            temp = "3 " + rows;
            resp += addLength(temp);
            response.put(resp + ",");
            reverseColumns(responseObject, cols, response, query);
        }

        if (requestObject.has("operations")) {
            JSONArray array = requestObject.getJSONArray("operations");
            boolean hasFetch = false;
            for(int i=0; i<array.length(); i++) {
                if (array.getString(i).contains("fetch")) {
                    hasFetch = true;
                    break;
                }
            }
            if (hasFetch) {
                response.put("6, ");
            }
        }

        if(responseObject.has("operationResponse")) {
            JSONArray array = responseObject.getJSONArray("operationResponse");
            for(int i=0; i<array.length(); i++){
                response.put(array.get(i) + ",");
            }
        }

        return response;
    }

    String reverseRequest(JSONObject requestObject) {
        StringBuilder responseString = new StringBuilder();
        if (requestObject.has("query")) {
            String q = reverseCommandMap.get("query") + " " +
                    requestObject.getString("query");
            responseString.append(q.length() + ":" + q);
        }
        if(requestObject.has("bindIn")) {
            JSONArray bindInArray = requestObject.getJSONArray("bindIn");

            for (int i = 0; i < bindInArray.length(); i++) {
                JSONObject bindInObject = bindInArray.getJSONObject(i);
                String bindString = "2 " + bindInObject.getString("name");
                responseString.append("," + bindString.length() + ":" + bindString);
                if (bindInObject.has("bindType")) {
                    bindString = "10 " + bindInObject.getString("bindType");
                    responseString.append("," + bindString.length() + ":" + bindString);
                }
                bindString = "3 " + bindInObject.getString("value");
                responseString.append("," + bindString.length() + ":" + bindString);
            }
        }
        if (requestObject.has("query"))
            responseString.append(",1:4");

        if(requestObject.has("operations")) {
            JSONArray tags = requestObject.getJSONArray("operations");
            for (int i = 0; i < tags.length(); i++) {
                String key = tags.getString(i);
                String val = String.valueOf(reverseCommandMap.get(key.split(",")[0]));
                if (key.contains(","))
                    val += " " + key.split(",")[1];
                if (reverseRawCommandMap.keySet().contains(key))
                    responseString.append(reverseRawCommandMap.get(key));
                else if (responseString.length() == 0 && val.startsWith("7 ")) {
                    responseString.append(val + ",");
                }
                else
                    responseString.append("," + val.length() + ":" + val);
            }
        }

        if (requestObject.has("query"))
            responseString.append(",,");

        return responseString.toString() ;
    }

    JSONObject parseRequest(String requestString) throws IOException {
        JSONObject response = new JSONObject();
        JSONArray tags = new JSONArray();
        JSONArray bindIn = new JSONArray();
        String parsedString = requestString;
        String previousKey = "";
        String bindType = "";

        while(parsedString.length() > 0 && !parsedString.equals(",") &&
                parsedString.contains(":")) {
            int len = Integer.valueOf(parsedString.split(":")[0]);
            Iterator<NetStringObj> iterator = parse(parsedString);
            parsedString = parsedString.substring(parsedString.indexOf(':')+1);
            parsedString = parsedString.substring(len+1);
            while(iterator.hasNext()) {
                NetStringObj obj = iterator.next();
                Long key = obj.getCommand();
                String data = new String(obj.getData());
                if(data.length() == 0)
                    data = "True";

                if (previousKey.length() > 0)
                    if(key.equals(3l)) {
                        JSONObject object = new JSONObject();
                        object.put("name", previousKey);
                        object.put("value", data);
                        if(!bindType.isEmpty()) {
                            object.put("bindType", bindType);
                            bindType = "";
                        }
                        bindIn.put(object);
                        previousKey = "";
                    } else if(key.equals(10l)) {
                        bindType = data;
                    } else {
                        throw new IOException("Unable to find bind value for " + previousKey);
                    }
                else if (key.equals(22l))
                    tags.put(commandMap.get(key));
                else if (key.equals(7l)) {
                    if (!data.equals("True"))
                        tags.put(commandMap.get(key) + "," + data);
                    else
                        tags.put(commandMap.get(key));
                }
                else if (key.equals(25l))
                    response.put(commandMap.get(key), data);
                else if (key.equals(2l) ) {
                    previousKey = data;
                }
                else if (key.equals(4l) ) {
                    //no operation - this is execute command
                }
                else{
                    throw new IOException("unknown command " + key);
                }

            }
        }
        if (parsedString.startsWith("7 ")) {
            Long key = Long.parseLong(parsedString.split(" ")[0]);
            String data = parsedString.split(" ")[1].split(",")[0];
            tags.put(commandMap.get(key) + "," + data);
            parsedString = "";
        }

        if(parsedString.length()>0 && !parsedString.equals(",") && !parsedString.contains(":")) {
            tags.put(rawCommandMap.get(parsedString));
        }
        if (tags.length() > 0)
            response.put("operations", tags);
        if(bindIn.length() > 0)
            response.put("bindIn", bindIn);
        return response;
    }

    private static void fillReferenceColumnNames(List<Column> columns, int numberOfColumns,
                                                 JSONObject requestObject, String query) {
        boolean columnCache = false;
        if (ColumnCache.cache.containsKey(query)
                && ColumnCache.cache.get(query).size() > 0){
            columnCache = true;
        }
        if ((columns == null || columns.size() <= 0) && !columnCache){
            List<Column> columnDetails = new ArrayList<Column>();
            for(int j=0; j<numberOfColumns; j++){
                Column column = new Column();
                column.setName("column_" + j);
                column.setIndex(j);
                columnDetails.add(column);
            }
            ColumnCache.cache.put(requestObject.getString("query"), columnDetails);
        }
    }

    JSONObject parseResponse(JSONArray responseList, JSONObject requestObject, Map<String, String> idQueryMap) throws IOException {
        int numberOfColumns = 0, numberOfRows = 0;
        ResponseDecoded responseDecoded = new ResponseDecoded();
        List<Column> columns = new ArrayList<Column>();
        responseDecoded.rows = new ArrayList();
        boolean requestedColumns = false;
        String query = "";
        if(requestObject.has("operations")) {
            JSONArray array = requestObject.getJSONArray("operations");
            for (int j = 0; j < array.length(); j++) {
                if (array.getString(j).equals("getColumn")) {
                    requestedColumns = true;
                    break;
                }
            }
        }
        for(int i=0; i<responseList.length(); i++) {
            String response =responseList.getString(i);
            Iterator<NetStringObj> iterator = parse(response.getBytes(StandardCharsets.UTF_8).length-1 + ":" + response);
            boolean onlyFetch = false;
            if(requestObject.has("query")){
                query = requestObject.getString("query");
            } else if (requestObject.has("operations")) {
                JSONArray op = requestObject.getJSONArray("operations");
                if (op.length() == 1 && op.getString(0).startsWith("fetch,")) {
                    query = idQueryMap.get(requestObject.getString("captureId"));
                    onlyFetch = true;
                }
            }
            if(query.length() > 0){
                if (i == 0 && !onlyFetch) {
                    NetStringObj object = iterator.next();
                    assert (object.getCommand() == 3);
                    numberOfColumns = Integer.valueOf(new String(object.getData()));
                    object = iterator.next();
                    assert (object.getCommand() == 3);
                    numberOfRows = Integer.valueOf(new String(object.getData()));
                } else if (requestedColumns && i == 1){
                    NetStringObj object = iterator.next();
                    assert (object.getCommand() == 3);
                    assert (Integer.valueOf(new String(object.getData())) == numberOfColumns);
                    for(int j=0; j<numberOfColumns; j++) {
                        Column column = new Column();
                        column.setName(new String(iterator.next().getData()));
                        column.setType(Integer.valueOf(new String(iterator.next().getData())));
                        column.setWidth(new String(iterator.next().getData()));
                        column.setPrecision(new String(iterator.next().getData()));
                        column.setScale(new String(iterator.next().getData()));
                        column.setIndex(j);
                        columns.add(column);
                    }
                    if(columns.size() > 0) {
                        ColumnCache.cache.put(requestObject.getString("query"),
                                columns);
                    }
                    responseDecoded.setColumns(columns);
                } else if(i != responseList.length()-1){
                    Map<String, String> row = new HashMap<String, String>();
                    if (columns.size() == 0)
                    {
                        columns = ColumnCache.cache.get(query);
                    }
                    if (numberOfColumns == 0)
                        numberOfColumns = columns.size();
                    for(int j=0; j<numberOfColumns; j++) {
                        row.put(columns.get(j).name, new String(iterator.next().getData()));
                    }
                    responseDecoded.getRows().add(row);
                }
                if(!requestedColumns)
                    fillReferenceColumnNames(columns, numberOfColumns, requestObject, query);
            } else{
                responseDecoded.operationResponse.add(iterator.next().getCommand());
            }
        }

        if(query.length() > 0 && responseDecoded.rows.size() == 0){
            responseDecoded.recordsAffected.put("columns", numberOfColumns);
            responseDecoded.recordsAffected.put("rows", numberOfRows);
        }

        return responseDecoded.getJson();
    }
}

class ResponseDecoded {
    List<Map<String, String>> rows;
    List<Column> columns;
    JSONObject recordsAffected = new JSONObject();
    List<Long> operationResponse = new ArrayList<Long>();

    public JSONObject getJson() {
        JSONObject object = new JSONObject();
        JSONArray array = new JSONArray();
        if(columns != null) {
            for(Column column: columns) {
                array.put(column.getJson());
            }
            object.put("columns", array);
        }
        if (rows != null && rows.size() > 0)
            object.put("rows", rows);
        if(recordsAffected.keySet().size() > 0)
            object.put("recordsAffected", recordsAffected);
        if(operationResponse.size() > 0)
            object.put("operationResponse", operationResponse);
        return object;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public void setRows(List<Map<String, String>> rows) {
        this.rows = rows;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }
}

class Column {
    String name;
    String type;
    String width;
    String precision;
    String scale;
    int index;

    public JSONObject getJson() {
        JSONObject object = new JSONObject();
        object.put("ColumnName", name);
        object.put("width", width);
        object.put("type", type);
        object.put("precision", precision);
        object.put("scale", scale);
        return object;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public int getType() {
        return HERADataTypes.reverseTypeMap.get(type);
    }
    public void setType(int type) {
        this.type = HERADataTypes.typeMap.get(type);
    }

    public String getWidth() {
        return width;
    }

    public void setPrecision(String precision) {
        this.precision = precision;
    }

    public void setScale(String scale) {
        this.scale = scale;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setWidth(String width) {
        this.width = width;
    }

    public String getPrecision() {
        return precision;
    }

    public String getScale() {
        return scale;
    }
}

class ColumnCache {
    static Map<String, List<Column>> cache = new HashMap<String, List<Column>>();

    static JSONArray getArray(String key){
        JSONArray jsonArray = new JSONArray();
        if(cache.containsKey(key)) {
            for (Column c : cache.get(key)) {
                jsonArray.put(c.getJson());
            }
        }

        return jsonArray;
    }
}
