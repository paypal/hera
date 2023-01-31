package com.paypal.hera.micrometer;

public class MeterInfoTest{
    private String name;
    private String db_host;
    private String sql_hash;
    private Object value;

    public MeterInfoTest(String name, String db_host, String sql_hash, Object value){
        this.name = name;
        this.db_host = db_host;
        this.sql_hash = sql_hash;
        this.value = value;
    }

    public String getName(){
        return this.name;
    }

    public String getHost(){
        return this.db_host;
    }

    public String getSqlHash(){
        return this.sql_hash;
    }

    public Object getValue(){
        return this.value;
    }
}

