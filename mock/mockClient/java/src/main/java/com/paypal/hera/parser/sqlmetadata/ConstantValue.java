package com.paypal.hera.parser.sqlmetadata;

public class ConstantValue {
    private String value;
    private String type;

    public String getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "ConstantValue{" +
                "value='" + value + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
