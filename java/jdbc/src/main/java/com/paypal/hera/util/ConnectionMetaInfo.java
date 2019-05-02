package com.paypal.hera.util;

/**
 * Created by alwjoseph on 8/13/18.
 */
public class ConnectionMetaInfo {
    private String serverBoxName;

    public String getServerBoxName() {
        return serverBoxName;
    }

    public void setServerBoxName(String serverBoxName) {
        this.serverBoxName = serverBoxName;
    }

    public String toString(){
        return String.format("{Connection Info:{OccBoxName:%s}}",serverBoxName);
    }


}
