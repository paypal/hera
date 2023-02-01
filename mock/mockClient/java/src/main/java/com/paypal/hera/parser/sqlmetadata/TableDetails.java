package com.paypal.hera.parser.sqlmetadata;

public class TableDetails {
    private String tableName;
    private String aliasName;

    public String getAliasName() {
        return aliasName;
    }

    public String getTableName() {
        return tableName;
    }

    public TableDetails(String tableName, String aliasName) {
        this.aliasName = aliasName;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "TableDetails{" +
                "tableName='" + tableName + '\'' +
                ", aliasName='" + aliasName + '\'' +
                '}';
    }
}
