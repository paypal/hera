package com.paypal.dal.parser.sqlmetadata;

public class SelectItemMetaData {

    private String columnName;
    private String columnType;
    private String tableName;
    private int selectLevel;
    private ConstantValue constantValue;
    private String originalColumnName;

    public SelectItemMetaData(String columnName, String tableName, String columnType, int selectLevel,
                              String originalColumnName) {
        this.columnName = columnName;
        this.selectLevel = selectLevel;
        this.tableName = tableName;
        this.columnType = columnType;
        if(originalColumnName == null) {
            this.originalColumnName = columnName;
        } else {
            this.originalColumnName = originalColumnName;
        }
    }

    public ConstantValue getConstantValue() {
        return constantValue;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setSelectLevel(int selectLevel) {
        this.selectLevel = selectLevel;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setConstantValue(ConstantValue constantValue) {
        this.constantValue = constantValue;
    }

    public String getTableName() {
        return tableName;
    }

    public int getSelectLevel() {
        return selectLevel;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getOriginalColumnName() {
        return originalColumnName;
    }

    @Override
    public String toString() {
        return "SelectItemMetaData{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", tableName='" + tableName + '\'' +
                ", selectLevel=" + selectLevel +
                ", constantValue=" + constantValue +
                ", originalColumnName=" + originalColumnName +
                '}';
    }
}
