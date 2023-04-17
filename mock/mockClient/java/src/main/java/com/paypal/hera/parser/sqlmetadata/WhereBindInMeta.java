package com.paypal.hera.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class WhereBindInMeta {

    private String columnName;
    private List<String> variableName = new ArrayList<>();
    private String tableName;
    private ConstantValue constantValue;

    public String getTableName() {
        return tableName;
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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public List<String> getVariableName() {
        return variableName;
    }

    public void setVariableName(List<String> variableName) {
        this.variableName = variableName;
    }

    @Override
    public String toString() {
        return "WhereBindInMeta{" +
                "columnName='" + columnName + '\'' +
                ", variableName='" + variableName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", constantValue=" + constantValue +
                '}';
    }
}
