package com.paypal.dal.parser.sqlmetadata;

public class ColumnDetails {
    private String tableName;
    private String aliasName;
    private String columnName;

    public ColumnDetails(String tableName, String columnName, String aliasName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.aliasName = aliasName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getAliasName() {
        return aliasName;
    }

    @Override
    public String toString() {
        return "ColumnDetails{" +
                "tableName='" + tableName + '\'' +
                ", aliasName='" + aliasName + '\'' +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
