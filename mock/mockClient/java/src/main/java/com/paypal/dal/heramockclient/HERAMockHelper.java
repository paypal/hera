package com.paypal.dal.heramockclient;

import com.paypal.dal.heramockclient.mockannotation.JDBCMockConst;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

import static com.paypal.dal.heramockclient.HERAMockAction.ADD_MOCK_CONSTRAINT;
import static com.paypal.dal.heramockclient.HERAMockAction.NEXT_QUERY;
import static com.paypal.dal.heramockclient.mockannotation.JDBCMockConst.NEW_LINE;


public class HERAMockHelper {

    private static Map<String, Object> dataTypeMap = new HashMap<String, Object>(){
        {
            put("int", int.class);
            put("String", String.class);
            put("BigDecimal", BigDecimal.class);
            put("Long", Long.class);
        }
    };
    private static boolean decode = false;

    private static String getMockAddURL() {
        return "http://" + mockIP + ":13916/mock/add";
    }

    private static String getMockListURL() {
        return "http://" + mockIP + ":13916/mock/list";
    }

    private static String getRRCaptureList() {
        return "http://" + mockIP + ":13916/mock/getreqresp";
    }

    private static String getReplayStatus() {
        return "http://" + mockIP + ":13916/mock/status";
    }

    private static String getMockRemoveURL() {
        return "http://" + mockIP + ":13916/mock/delete";
    }

    private static String getMockReloadURL() {
        return "http://" + mockIP + ":13916/mock/reload";
    }

    private static String mockIP = "127.0.0.1";
    private static String previousServerIP = "";
    private static String defaultMockIPKey = "DALHERAMOCK_SERVERIP";
    private static String portMockIPKey = "PORT_DALHERAMOCK_SERVERIP";
    private static boolean portSpecificIPSpecified = false;
    private static boolean backupPreviousIP = false;

    public static void setDecode(boolean _decode) {
        decode = _decode;
    }
    public static boolean getDecode() {
        return decode;
    }

    public static void setMockIP(String mIP) {
        mockIP = mIP;
    }

    public static String getMockIP() {
        String value = System.getenv("MOCK_IP");
        System.out.println("MOCK_IP from env variable " + value);
        if (value != null) {
            System.out.println("Using MOCK_IP from env variable " + value);
            return value;
        }
        return mockIP;
    }

    public static Map<String, String> listMock() {
        Map<String, String> response = new HashMap<String, String>();
        try {
            URL url = new URL(getMockListURL());
            HttpURLConnection connection = getUrl(url);
            int respCode = connection.getResponseCode();
            if (respCode != HttpURLConnection.HTTP_OK) {
                System.out.println("unable to set the mock got response code as " + respCode);
                return response;
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String readline;
            while((readline = in.readLine()) != null) {
                String[] mocks = readline.split("=");
                if (mocks.length > 1)
                    response.put(mocks[0], mocks[1]);
                else
                    response.put(mocks[0], "");
            }

        }catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception while setting up mock " + ex.getMessage());
        }

        return response;
    }

    public static void disableLogging(){
        addMock(JDBCMockConst.DISABLE_LOG, "true", 1, -1);
    }

    public static void enableLogging(){
        removeMock(JDBCMockConst.DISABLE_LOG);
    }


    public static boolean reloadMock() {
        try {
            URL url = new URL(getMockReloadURL());
            HttpURLConnection connection = getUrl(url);
            int respCode = connection.getResponseCode();
            if (respCode != HttpURLConnection.HTTP_OK) {
                System.out.println("unable to reload the mock got response code as " + respCode);
                return false;
            }
            Thread.sleep(2000); // when changing server mock restart in 1 sec
            return true;

        }catch (Exception ex) {
            System.out.println("Exception while setting up mock " + ex.getMessage());
        }

        return false;
    }

    public static String getServerIp() {
        Map<String, String> mockList = listMock();
        return mockList.get(defaultMockIPKey);
    }

    public static boolean setServerIp(String newIp) {
        previousServerIP = getServerIp();
        backupPreviousIP = true;
        boolean resp = addMock(defaultMockIPKey, newIp);
        try {
            Thread.sleep(2000); // when changing server mock restart in 1 sec
        }catch (InterruptedException ignore) {
        }
        return resp;
    }

    public static boolean setServerIp(String newIp, String ports) {
        boolean resp = addMock(portMockIPKey, newIp + ":" + ports);
        try {
            Thread.sleep(2000); // when changing server mock restart in 1 sec
        }catch (InterruptedException ignore) {
        }
        portSpecificIPSpecified = true;
        return resp;
    }

    public static boolean revertServerIp() {
        boolean resp = false;
        if (backupPreviousIP) {
            resp = addMock(defaultMockIPKey, previousServerIP);
            previousServerIP = "";
            backupPreviousIP = false;
            try {
                Thread.sleep(2000); // when changing server mock restart in 1 sec
            } catch (InterruptedException ignore) {
            }
        }
        if(portSpecificIPSpecified) {
            resp = addMock(portMockIPKey, "");
        }
        return resp;
    }

    private static HttpURLConnection getUrl(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/text");
        connection.connect();
        return connection;
    }

    private static int doCall(HttpURLConnection connection, String params) throws IOException {
        connection.setDoOutput(true);
        OutputStream output = connection.getOutputStream();
        output.write(params.getBytes());
        output.flush();
        output.close();
        return connection.getResponseCode();
    }

    public static String getMock(String key) {
        Map<String, String> map = listMock();
        return map.get(key);
    }

    public static boolean addRandomFailureMock(String key, String successValue,
                                               String failureValue) {
        return addRandomFailureMock(key, successValue, failureValue, 25);
    }

    public static boolean addRandomFailureMock(String key, String successValue,
                                               String failureValue,
                                               int failurePercentage) {
        return addRandomFailureMock(key, successValue, failureValue, failurePercentage, 100);
    }

    public static boolean addRandomFailureMock(String key, String successValue,
                                               String failureValue,
                                               int failurePercentage,
                                               int failureSampling){
        if (failurePercentage > 100)
            failurePercentage = 100;

        if (failureSampling > 100)
            failureSampling = 100;

        return addMock(key,
                "FOREVER_RANDOM NEXT_COMMAND_REPLY " +
                        failurePercentage +
                        ADD_MOCK_CONSTRAINT +
                        failureSampling +
                        ADD_MOCK_CONSTRAINT +
                        successValue +
                        ADD_MOCK_CONSTRAINT +
                        failureValue
                );

    }

    public static boolean addMock(String key, String value)    {
        return addMock(key, value, 1);
    }

    public static boolean simulateCustomAuthConnectionFailure() {
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT, HERAMockAction.SIMULATE_AUTH_FAILURE, 1);
    }

    public static boolean simulateCustomAuthConnectionFailure(String port) {
        if (port.equals("")) {
            return simulateCustomAuthConnectionFailure();
        }
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT + ":" + port, HERAMockAction.SIMULATE_AUTH_FAILURE, 1);
    }

    public static boolean simulateConnectionTimeout() {
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT, HERAMockAction.CONNECTION_TIMEOUT, 1);
    }

    public static boolean simulateConnectionTimeout(int timeout) {
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT, HERAMockAction.CONNECTION_TIMEOUT, 1, timeout);
    }

    public static boolean simulateConnectionTimeout(String port) {
        if (port.equals("")) {
            return simulateConnectionTimeout();
        }
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT + ":" + port, HERAMockAction.CONNECTION_TIMEOUT, 1);
    }

    public static boolean simulateConnectionClockSkew() {
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT, HERAMockAction.SIMULATE_CLOCK_SKEW, 1);
    }

    public static boolean simulateConnectionClockSkew(String port) {
        if (port.equals("")) {
            return simulateConnectionClockSkew();
        }
//        addMock("ip", "MOCK");
        return addMock(HERAMockAction.ACCEPT + ":" + port, HERAMockAction.SIMULATE_CLOCK_SKEW, 1);
    }

    public static boolean removeConnectionMock() {
//        removeMock("ip");
        return removeMock(HERAMockAction.ACCEPT);
    }

    public static boolean removeConnectionMock(String mockKey) {
        if(mockKey.equals(""))
            return removeConnectionMock();
//        removeMock("ip");
        return removeMock(HERAMockAction.ACCEPT + ':' + mockKey);
    }

    public static boolean startRRCapture(String key) {
        return addMock(key, HERAMockAction.CAPTURE);
    }

    public static boolean replayCaptured(String key, String fileName) throws IOException{
//        addMock("ip", "MOCK");
        return addMock(key, readFromJson(fileName));
    }

    public static boolean replayCaptured(String key, String fileName, int timeout) throws IOException{
//        addMock("ip", "MOCK");
        return addMock(key, readFromJson(fileName), 1, timeout);
    }

    private static String getSingleValue(String val) {
        if(JDBCMockConst.getCmd(val).equals(val)) {
            String tmp = (JDBCMockConst.VAL + val).trim();
            return tmp.length() + JDBCMockConst.SEP + tmp + JDBCMockConst.NEXT;
        } else {
            return JDBCMockConst.NEXT + NEW_LINE + JDBCMockConst.getCmd(val);
        }
    }

    private static int getRowCnt(String first) {
        if (first.startsWith("UPDATED ") && first.endsWith(" ROWS"))
            return Integer.valueOf(first.split("UPDATED ")[1].split(" ROWS")[0]);
        else
            return 0;
    }

    public static String readFromJson(String fileName) throws IOException {
        InputStream is = new FileInputStream(fileName);
        Reader reader = new BufferedReader(new InputStreamReader(is));
        JSONTokener jsonTokener = new JSONTokener(reader);
        JSONObject jsonObject = new JSONObject(jsonTokener);
        JSONArray jsonArray = (JSONArray) jsonObject.get(HERAMockAction.CAPTURE);
        StringBuilder resp = new StringBuilder();
        for(int i=0;i<jsonArray.length();i++){
            JSONObject e = (JSONObject) jsonArray.get(i);
            resp.append(JDBCMockConst.REPLAY_REQ + JDBCMockConst.REQUEST);
            if(e.has("captureId")) {
                resp.append(e.getString("captureId")).append(" HERAMOCK_CAPTURE_ID ");
            }
            if(e.has("heraPort")) {
                resp.append(e.getString("heraPort")).append(" HERAMOCK_PORT ");
            }
            if(e.has("rawRequest")) {
                resp.append(JDBCMockConst.getCmd(e.getString("rawRequest")));
            }
            else {
                resp.append(new NSReader().reverseRequest(e.getJSONObject("request")));
            }

            JSONArray response;
            if(!e.has("rawResponse")) {
                response = new NSReader().reverseResponse(e.getJSONObject("response"),
                        e.getJSONObject("request"));
            } else {
                response = e.getJSONArray("rawResponse");
            }
            boolean firstResponse = true;
            for (int j = 0; j < response.length(); j++) {
                if (firstResponse) {
                    resp.append(JDBCMockConst.RESPONSE);
                    firstResponse = false;
                } else {
                    resp.append(NEW_LINE);
                }
                resp.append(JDBCMockConst.getCmd(response.getString(j)));
            }
        }
        return resp.toString();
    }

    private static List<Integer> getRowsCols(String request, List<String> responses) {
        List<Integer> resp = new ArrayList<Integer>();
        String r = responses.get(0);
        if(request.contains(":") && request.split(":")[1].split(" ")[0].equals("25")) {
            if(r.startsWith("0") && r.split(":").length == 3) {
                int cols = Integer.valueOf(r.split(":")[1].split(" ")[1].split(",")[0]);
                int rows = Integer.valueOf(
                        r.split(":")[2].split(" ")[1].split(",")[0]);
                resp.add(cols);
                resp.add(rows);
            }
        } else{
            resp.add(0);
            resp.add(0);
        }
        return resp;
    }

    private static List<List> splitRows(String request, List<String> responses) {
        List<List> rows = new ArrayList<List>();
        if(request.contains(":") && request.split(":")[1].split(" ")[0].equals("25")) {
            List<Integer> details = getRowsCols(request, responses);
            int i = 0;
            int response_idx = 1;
            if (details.get(1) == 0) {
                if (request.contains(",1:4,2:22,")) {
                    List<String> row = new ArrayList<String>();
                    row.add(responses.get(response_idx));
                    rows.add(row);
                    response_idx = 2;
                }
                String[] values = responses.get(response_idx).split(":");
                while(values.length > i+1) {
                    i += 1;
                    List<String> out = new ArrayList<String>();
                    for (; ; i++) {
                        String key = values[i].split(",")[0];
                        String val = "";
                        if (key.length() > 1) {
                            val = key.split(" ")[1];
                        }
                        out.add(val);
                        if (i % details.get(0) == 0)
                            break;
                    }
                    rows.add(out);
                }
                if(responses.size() > response_idx)
                    rows.add(Collections.singletonList(JDBCMockConst.getText(responses.get(response_idx+1))));
            } else{
                rows.add(Collections.singletonList("UPDATED " + details.get(1) + " ROWS"));
            }
        }
        return rows;
    }

    static void writeToJSON(String fileName, String content) throws IOException{
        JSONObject output = new JSONObject();
        JSONArray reqRespList = new JSONArray();
        Map<String, String> idQueryMap = new HashMap<>();
        for(String reqResp : content.split(JDBCMockConst.REPLAY_REQ + JDBCMockConst.REQUEST)) {
            if(reqResp.trim().length() == 0)
                continue;
            JSONObject requestResponse = new JSONObject();
            JSONObject decodedRequest = new JSONObject();
            String[] resp = reqResp.split(JDBCMockConst.RESPONSE);
            String rawRequest = resp[0].trim();
            String heraMockCaptureId = "Unknown";
            String heraMockPort = "Unknown";
            if (rawRequest.contains(" HERAMOCK_CAPTURE_ID ")) {
                heraMockCaptureId = rawRequest.split(" HERAMOCK_CAPTURE_ID ")[0].trim();
                rawRequest = rawRequest.split(" HERAMOCK_CAPTURE_ID ")[1].trim();
            }
            if (rawRequest.contains("HERAMOCK_PORT ")) {
                heraMockPort = rawRequest.split("HERAMOCK_PORT ")[0].trim();
                rawRequest = rawRequest.split("HERAMOCK_PORT ")[1].trim();
            }
            if (decode) {
                decodedRequest = new NSReader().parseRequest(rawRequest);
                requestResponse.put("request", decodedRequest);
                if (decodedRequest.has("query"))
                    idQueryMap.put(heraMockCaptureId, decodedRequest.getString("query"));
                decodedRequest.put("captureId", heraMockCaptureId);
            }
            else {
                requestResponse.put("rawRequest", rawRequest);
            }
            requestResponse.put("captureId", heraMockCaptureId);
            requestResponse.put("heraPort", heraMockPort);
            JSONArray responses = new JSONArray();
            if (resp.length > 1) {
                for (String key : resp[1].split(NEW_LINE)) {
                    responses.put(key.trim());
                }
            }

            if(decode) {
                requestResponse.put("response", new NSReader().parseResponse(responses, decodedRequest, idQueryMap));
            } else {
                requestResponse.put("rawResponse", responses);
            }
            reqRespList.put(requestResponse);
        }
        output.put(HERAMockAction.CAPTURE, reqRespList);

        //Write JSON file
        FileWriter file = new FileWriter(fileName);
        file.write(output.toString(4));
        file.flush();
        file.close();

    }

    public static String getReplayStatus(String key) throws IOException {
        StringBuilder content = new StringBuilder();
        String params = "key=" + key;
        URL url = new URL(getReplayStatus() + "?" + params);
        HttpURLConnection connection = getUrl(url);
        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            return "unknown";
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String readline;
        while((readline = in.readLine()) != null) {
            content.append(readline);
        }
        return content.toString();
    }

    public static void endRRCapture(String key, String outFile) throws IOException {
        StringBuilder content = new StringBuilder();
        String params = "key=" + key;
        URL url = new URL(getRRCaptureList() + "?" + params);
        HttpURLConnection connection = getUrl(url);
        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            System.out.println("unable to capture the RR " + connection.getResponseCode());
            return;
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String readline;
        while((readline = in.readLine()) != null) {
            content.append(readline);
        }
        writeToJSON(outFile, content.toString());
    }

    public static boolean addMockForEver(String key, String value) {
        return addMock(key, value, -1, -1);
    }

    public static boolean mockCommitForEver() {
        return addMock(JDBCMockConst.MOCK_COMMIT_FOREVER, "5,", 1, -1);
    }

    public static boolean mockRollbackForEver() {
        return addMock(JDBCMockConst.MOCK_ROLLBACK_FOREVER, "5,", 1, -1);
    }

    public static boolean captureSQLRequestResponse(String sqlName){
        return addMock(sqlName, "CAPTURE_SQL,");
    }

    static String toProperCase(String s) {
        return s.substring(0, 1).toUpperCase() +
                s.substring(1).toLowerCase();
    }

    static String toCamelCase(String s){
        String[] parts = s.split("_");
        StringBuilder camelCaseString = new StringBuilder();
        for (String part : parts){
            camelCaseString.append(toProperCase(part));
        }
        return camelCaseString.toString();
    }

    private static String getData(String key) throws IOException{
        StringBuilder content = new StringBuilder();
        String params = "key=CAPTURE_SQL," + key;
        URL url = new URL(getRRCaptureList() + "?" + params);
        HttpURLConnection connection = getUrl(url);
        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            System.out.println("unable to capture the RR " + connection.getResponseCode());
            return null;
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String readline;
        while((readline = in.readLine()) != null) {
            content.append(readline);
        }
        return content.toString();
    }

    private static Map<String, Method> getSetOrGet(Object object, String key, int len) {
        Map<String, Method> map = new HashMap<String, Method>();
        for(Method m : object.getClass().getMethods()){
            if(m.getParameterTypes().length == len && m.getName().startsWith(key))
                map.put(m.getName(), m);
        }
        return map;
    }

    private static String addLength(String inp) {
        return inp.length() + ":" + inp + ",";
    }

    private static String prepareColumnDetails(JSONArray columns) {
        StringBuilder response = new StringBuilder();
        List<String> meta = Arrays.asList("ColumnName", "type", "width", "precision", "scale");
        for(int i=0; i<columns.length(); i++) {
            JSONObject object = columns.getJSONObject(i);
            if (i == 0) {
                String temp = "3 " + columns.length();
                response.append("0 " + addLength(temp));
                temp = "3 0";
                response.append(addLength(temp) + ",");
            }
            if (object.getString("ColumnName").equals("column_" + i))
                break; // no col info requested
            if (i==0) {
                response.append(" NEXT_NEWSTRING ");
                String temp = "3 " + columns.length();
                response.append("0 " + addLength(temp));
            }
            for(String m: meta) {
                String temp = "3";
                if(m.equals("type"))
                    temp += " " + HERADataTypes.reverseTypeMap.get(object.getString(m));
                else
                    temp += " " + object.getString(m);
                response.append(addLength(temp));
            }
        }
        return response.toString() + ",";
    }

    private static String prepareRows(Object[] objects, JSONArray columns) throws Exception{
        StringBuilder rows = new StringBuilder();
        rows.append("0 ");
        Map<String, Method> map = getSetOrGet(objects[0], "get", 0);
        for(Object object : objects) {
            for(int i=0; i<columns.length(); i++) {
                String name = "get" + toCamelCase(columns.getJSONObject(i).getString("ColumnName"));
                Object value = map.get(name).invoke(object);
                String temp = "3";
                if(value != null)
                    temp += " " + String.valueOf(value);
                rows.append(addLength(temp));
            }
        }
        return rows.toString() + ",";
    }

    private static void prepareResponse(JSONArray rows, Object[] objects) throws Exception{
        for(int i=0;i<rows.length();i++){
            JSONObject row = rows.getJSONObject(i);
            Object object = objects[i];
            Set<String> keys = row.keySet();
            for(String k : keys){
                Map<String, Method> map = getSetOrGet(object, "set", 1);
                String name = "set" + toCamelCase(k);
                if (!map.containsKey(name))
                    throw new HERAMockException("unable to find setter for " + k);
                Method m = map.get(name);
                if (m.getParameterTypes()[0].getSimpleName().equals(int.class.getSimpleName()))
                    m.invoke(object, row.getInt(k));
                else if (m.getParameterTypes()[0].getSimpleName().equals(long.class.getSimpleName()))
                    m.invoke(object, row.getLong(k));
                else if (m.getParameterTypes()[0].getSimpleName().equals(double.class.getSimpleName()))
                    m.invoke(object, row.getDouble(k));
                else if (m.getParameterTypes()[0].getSimpleName().equals(BigInteger.class.getSimpleName()))
                    m.invoke(object, BigInteger.valueOf(row.getLong(k)));
                else if (m.getParameterTypes()[0].getSimpleName().equals(BigDecimal.class.getSimpleName()))
                    m.invoke(object, BigDecimal.valueOf(row.getDouble(k)));
                else
                    m.invoke(object, row.getString(k));
            }
        }
    }

    public static String mockQuery(String mockKey, Object[] mockResponses,
                                 JSONObject object) throws Exception {
        String mockResponse = prepareColumnDetails(object.getJSONArray("columns"));
        mockResponse += " NEXT_NEWSTRING " + prepareRows(mockResponses, object.getJSONArray("columns"));
        mockResponse += " NEXT_NEWSTRING 6,";
        addMock(mockKey, mockResponse);
        return mockResponse;
    }


    public static JSONObject getCapturedSQLResponse(String key, Object[] objects) throws Exception {
        Map<String, String> map = getCapturedSQLResponse(key);
        NSReader nsReader = new NSReader();
        String request = map.get(key);
        if(request.contains("CorrId=")){
            request = request.split("CorrId=")[1];
            String temp = request.split(":")[0].split(",")[1];
            request = temp + request.substring(request.indexOf(':'));
        }

        String heraMockPort = "Unknown";
        String heraMockCaptureId = "Unknown";

        if (request.contains(" HERAMOCK_CAPTURE_ID ")) {
            heraMockCaptureId = request.split(" HERAMOCK_CAPTURE_ID ")[0].trim();
            request = request.split(" HERAMOCK_CAPTURE_ID ")[1].trim();
        }
        if (request.contains("HERAMOCK_PORT ")) {
            heraMockPort = request.split("HERAMOCK_PORT ")[0].trim();
            request = request.split("HERAMOCK_PORT ")[1].trim();
        }

        JSONObject requestObject = nsReader.parseRequest(request);
        JSONArray responseArray = new JSONArray();
        for (String temp : map.get(key + "_RESPONSE").split(" NEXT_NEWSTRING ")) {
            responseArray.put(temp);
        }

        Map<String, String> idQueryMap = new HashMap<>();
        if (requestObject.has("query") && !idQueryMap.containsKey(heraMockCaptureId))
            idQueryMap.put(heraMockCaptureId, requestObject.getString("query"));
        JSONObject responseObject = nsReader.parseResponse(responseArray, requestObject, idQueryMap);
        if (responseObject.has("rows")) {
            JSONArray rows = responseObject.getJSONArray("rows");
            if (rows.length() > objects.length) {
                throw new HERAMockException("got " + rows.length() + " number of rows returned " +
                        "but allocated objects in input is " + objects.length);
            }
            prepareResponse(rows, objects);
            requestObject.put("columns", ColumnCache.getArray(requestObject.getString("query")));
        } else if (responseObject.has("recordsAffected")) {
            int rows =  responseObject.getJSONObject("recordsAffected").getInt("rows");
            objects[0] = rows;
        } else{
            String query = "Unknown";
            if (requestObject.has("query"))
                query = requestObject.getString("query");
            throw new HERAMockException("there is no response for  " + query);
        }

        return requestObject;
    }

    public static Map<String, String> getCapturedSQLResponse(String key) throws IOException {
        Map<String, String> resp = new HashMap<String, String>();
        String data = getData(key);
        resp.put(key, data.split(" START_RESPONSE ")[0]);
        if (data.split(" START_RESPONSE ").length > 1) {
            resp.put(key + "_RESPONSE", data.split(" START_RESPONSE ")[1]);
        } else {
            resp.put(key + "_RESPONSE", "something went wrong");
        }
        return resp;
    }

    public static boolean addMock(String key, String value, int nThOccurance) {
        return addMock(key, value, nThOccurance, 120);
    }

    public static boolean addMock(String key, String value, int nThOccurance, int timeout) {
        return addMock(key, value, nThOccurance, timeout, 0);
    }

    public static boolean addMock(String key, Object objectToRespond, int nThOccurance) throws HERAMockException {
        return addMock(key, objectToRespond, nThOccurance, 120);
    }

    public static boolean addMock(String key, Object objectToRespond, int nThOccurance, int timeout) throws HERAMockException {
        return addMock(key, objectToRespond, nThOccurance, timeout, 0);
    }

    public static boolean addMock(String key, Object objectToRespond) throws HERAMockException {
        return addMock(key, objectToRespond, 1);
    }

    private static void getSingleObjectMock(Object objectToRespond,
                                              StringBuilder value,
                                              StringBuilder columnMeta,
                                              boolean isFirst) throws HERAMockException {
        try {
            Field[] fields = objectToRespond.getClass().getDeclaredFields();
            for (Field f : fields) {
                if (Modifier.isStatic(f.getModifiers()))
                    continue;
                if (isFirst)
                    columnMeta.append(DataTypeMetaMap.getEquivalent(f.getName(), f.getType().getSimpleName()));
                f.setAccessible(true);
                String fieldValue;
                if (f.get(objectToRespond) != null) {
                    String bufferCase = DataTypeMetaMap.variableCaseToBufferCase(f.getName());
                    String fieldStart = bufferCase + "_START_HERA_MOCK ";
                    String fieldEnd = " " + bufferCase + "_END_HERA_MOCK ";
                    fieldValue = f.get(objectToRespond).toString();
                    value.append(fieldStart + fieldValue + fieldEnd);
                }
            }
        }catch (IllegalAccessException e) {
            throw new HERAMockException(e);
        }
    }

    public static String getObjectMock(Object objectToRespond, int delayMs) throws HERAMockException {
        boolean firstObject = true;
        StringBuilder value = new StringBuilder();
        StringBuilder columnMeta = new StringBuilder();
        if(objectToRespond.getClass().getSuperclass().getSimpleName().equals(AbstractList.class.getSimpleName())) {
            List objects = (List)objectToRespond;
            for(Object obj : objects) {
                getSingleObjectMock(obj, value, columnMeta, firstObject);
                firstObject = false;
                value.append(NEW_LINE);
            }
        } else {
            getSingleObjectMock(objectToRespond, value, columnMeta, true);
            value.append(NEW_LINE);
        }
        String firstLine = "";
        if(delayMs > 0)
            firstLine = delayMs + JDBCMockConst.MOCK_DELAYED_RESPONSE;
        firstLine += "HERAMOCK_OBJECT_MOCK_META ";
        return  firstLine + columnMeta  +
                NEW_LINE + value;
    }

    public static boolean loadBasedMock(String port,
                                        List<LoadBasedMock> loadBasedMocks) {
        StringBuilder mockValue = new StringBuilder();
        for(LoadBasedMock loadBasedMock : loadBasedMocks) {
            if(mockValue.length() > 0)
                mockValue.append(JDBCMockConst.HERAMOCK_TABLESEP);
            mockValue.append(loadBasedMock.getMinRange()).append("=").append(loadBasedMock.getMaxRange());
            mockValue.append(JDBCMockConst.HERAMOCK_TABLESEP).append(loadBasedMock.getKey()).append(JDBCMockConst.HERAMOCK_TABLESEP);
            mockValue.append(loadBasedMock.getFailurePercentage()).append(JDBCMockConst.HERAMOCK_TABLESEP);
            mockValue.append(loadBasedMock.getSuccessResponse()).append(JDBCMockConst.HERAMOCK_TABLESEP);
            mockValue.append(loadBasedMock.getFailureResponse());
        }
        return HERAMockHelper.addMock(port, JDBCMockConst.LOAD_BASED_MOCK + mockValue,
                0, -1);
    }

    public static boolean addMock(String key, Object objectToRespond, int nThOccurance, int timeout, int delayMs) throws HERAMockException {

        try {
            addMock(key, getObjectMock(objectToRespond, delayMs), nThOccurance, timeout);
            return true;
        }catch (Exception e) {
            throw new HERAMockException(e);
        }
    }

    public static boolean addMock(String key, String value, int nThOccurance, int timeout, int delayInMs) {
        boolean response = true;
        StringBuilder newValue = new StringBuilder();
        if(delayInMs > 0) {
            newValue.append(delayInMs).append(JDBCMockConst.MOCK_DELAYED_RESPONSE);
        }
        for(int i=1; i<nThOccurance; i++){
            newValue.append("NOMOCK NEXT_COMMAND_REPLY ");
        }
        if(nThOccurance == -1){
            newValue.append(JDBCMockConst.FOREVER);
        } else if (nThOccurance < -1) {
            for(int i=0; i<Math.abs(nThOccurance)-1; i++){
                newValue.append(value + JDBCMockConst.NEXT + NEXT_QUERY + " ");
            }
        }
        newValue.append(value);
        try {
            String params = key.replace("=", "heraMockEqual") + "=" +
                    newValue.toString().replace("=", "heraMockEqual");

            String timeoutKey = "expire_time_in_sec";
            params += "&" + timeoutKey + "=" + timeout;
            URL url = new URL(getMockAddURL());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");

            int respCode = doCall(connection, params);
            if (respCode != HttpURLConnection.HTTP_OK) {
                response = false;
                System.out.println("unable to set the mock got response code as " + respCode);
            }
        }catch (Exception ex) {
            response = false;
            System.out.println("Exception while setting up mock " + ex.getMessage());
        }

        return response;
    }

    public static boolean removeMock(String key) {
        boolean response = true;
        try{
            String params = "key=" + key;
            URL url = new URL(getMockRemoveURL() + "?" + params);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            int respCode = doCall(connection, params);
            if (respCode != HttpURLConnection.HTTP_OK) {
                response = false;
                System.out.println("unable to delete the mock got response code as " + respCode);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            response = false;
        }
        return response;
    }
}
