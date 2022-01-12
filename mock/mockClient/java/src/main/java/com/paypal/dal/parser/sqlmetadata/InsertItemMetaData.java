package com.paypal.dal.parser.sqlmetadata;

public class InsertItemMetaData {
    private String columnName;
    private String tableName;
    private String variableName;
    private ConstantValue constantValue;
    private boolean bindIn = true;

    public boolean isBindIn() {
        return bindIn;
    }


    public void setBindIn(boolean bindIn) {
        this.bindIn = bindIn;
    }

    public InsertItemMetaData(String tableName, String columnName) {
        this.columnName = columnName;
        this.tableName = tableName;
    }
    public ConstantValue getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(ConstantValue constantValue) {
        this.constantValue = constantValue;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getVariableName() {
        return variableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    @Override
    public String toString() {
        return "InsertItemMetaData{" +
                "columnName='" + columnName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", variableName='" + variableName + '\'' +
                ", constantValue=" + constantValue +
                '}';
    }
}
