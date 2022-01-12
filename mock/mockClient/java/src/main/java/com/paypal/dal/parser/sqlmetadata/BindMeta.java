package com.paypal.dal.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class BindMeta {
    public BindMeta(boolean isOracleSql) {
        this.isOracleSql = isOracleSql;
    }
    private boolean isOracleSql;
    private String columnName;
    private String tableName;
    private List<String> bindValues = new ArrayList<>();
    private List<String> bindConstants = new ArrayList<>();
    List<String> updateValues = new ArrayList<>();
    private boolean inClause;
    private boolean isBindValueDynamic = true;

    @Override
    public String toString() {
        return "BindMeta{" +
                "isOracleSql=" + isOracleSql +
                ", columnName='" + columnName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", bindValues=" + bindValues +
                ", bindConstants=" + bindConstants +
                ", updateValues=" + updateValues +
                ", inClause=" + inClause +
                ", isBindValueDynamic=" + isBindValueDynamic +
                '}';
    }

    public List<String> getUpdateValues() {
        return updateValues;
    }

    public void setUpdateValues(List<String> updateValues) {
        this.updateValues = updateValues;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        if(isOracleSql)
            columnName = columnName.toUpperCase();
        this.columnName = columnName;
    }

    public void setTableName(String tableName) {
        if(isOracleSql)
            tableName = tableName.toUpperCase();
        this.tableName = tableName;
    }

    public void addBindConstant(String newValue) {
        this.bindConstants.add(newValue);
        this.isBindValueDynamic = false;
    }

    public void addBindValue(String newValue) {
        this.bindValues.add(newValue);
        this.isBindValueDynamic = false;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getBindValues() {
        return bindValues;
    }

    public List<String> getBindConstants() {
        return bindConstants;
    }

    public void setBindValues(List<String> bindValues) {
        this.bindValues = bindValues;
        this.isBindValueDynamic = false;
    }


    public boolean isInClause() {
        return inClause;
    }

    public void setInClause(boolean inClause) {
        this.inClause = inClause;
    }

    public void setBindConstants(List<String> bindConstants) {
        this.bindConstants = bindConstants;
        this.isBindValueDynamic = false;
    }
}
