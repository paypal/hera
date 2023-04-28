package com.paypal.hera.heramockclient;

import java.util.List;
import java.util.Map;

public class MockLog {
    String queryTime;
    Double epocTime;
    String SQL;
    String port;
    String heraName;
    Map<String, String> bindIn;
    List<Map<String, String>> bindOut;

    private String mapToString(Map<String, String> map) {
        StringBuilder resp = new StringBuilder();
        if(map == null)
            return resp.toString();
        for(String key: map.keySet()) {
            resp.append(key).append(":").append(map.get(key)).append(",");
        }
        return resp.toString();
    }

    private String mapToString(List<Map<String, String>> map) {
        StringBuilder resp= new StringBuilder();
        if(map == null)
            return resp.toString();
        for(Map<String, String> m : map)
            resp.append(mapToString(m));
        return resp.toString();
    }

    public void setBindIn(Map<String, String> bindIn) {
        this.bindIn = bindIn;
    }

    public List<Map<String, String>> getBindOut() {
        return bindOut;
    }

    public void setBindOut(List<Map<String, String>> bindOut) {
        this.bindOut = bindOut;
    }

    public void setHeraName(String heraName) {
        this.heraName = heraName;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public Map<String, String> getBindIn() {
        return bindIn;
    }

    public String getHeraName() {
        return heraName;
    }

    public String getPort() {
        return port;
    }

    public String getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(String queryTime) {
        this.queryTime = queryTime;
    }

    public Double getEpocTime() {
        return epocTime;
    }

    public void setEpocTime(Double epocTime) {
        this.epocTime = epocTime;
    }

    public String getSQL() {
        return SQL;
    }

    public void setSQL(String SQL) {
        this.SQL = SQL;
    }

    public String getCorrId() {
        return CorrId;
    }

    public void setCorrId(String corrId) {
        CorrId = corrId;
    }

    String CorrId;

    @Override
    public String toString() {
        return "MockLog{" +
                "queryTime='" + queryTime + '\'' +
                ", epocTime=" + epocTime +
                ", SQL='" + SQL + '\'' +
                ", port='" + port + '\'' +
                ", heraName='" + heraName + '\'' +
                ", bindIn=" + mapToString(bindIn) +
                ", bindOut=" + mapToString(bindOut) +
                ", CorrId='" + CorrId + '\'' +
                '}';
    }
}
