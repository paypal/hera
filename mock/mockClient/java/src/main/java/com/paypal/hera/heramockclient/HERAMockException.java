package com.paypal.hera.heramockclient;

public class HERAMockException extends Exception{
    String msg = "";
    public HERAMockException(Exception e) {
        super(e);
    }

    public HERAMockException(String msg){
        this.msg = msg;
    }
}
