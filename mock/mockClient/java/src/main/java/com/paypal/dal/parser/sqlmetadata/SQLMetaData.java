package com.paypal.dal.parser.sqlmetadata;


public class SQLMetaData {
    private String sqlType;
    private String sql;
    private SelectMetaData selectMetaData = new SelectMetaData();
    private TableMetaData tableMetaData = new TableMetaData();
    private ColumnMetaData columnMetaData = new ColumnMetaData();
    private InsertMetaData insertMetaData = new InsertMetaData();
    private UpdateMetaData updateMetaData = new UpdateMetaData();
    private DeleteMetaData deleteMetaData = new DeleteMetaData();
    private MergeMetaData mergeMetaData = new MergeMetaData();
    private boolean processingLeftExpression = true;

    public void setProcessingLeftExpression(boolean processingLeftExpression) {
        this.processingLeftExpression = processingLeftExpression;
    }

    public boolean isProcessingLeftExpression() {
        return processingLeftExpression;
    }

    public SQLMetaData(String sql) {
        this.sql = sql;
    }

    public UpdateMetaData getUpdateMetaData() {
        return updateMetaData;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }

    public InsertMetaData getInsertMetaData() {
        return insertMetaData;
    }


    public MergeMetaData getMergeMetaData() {
        return mergeMetaData;
    }

    public DeleteMetaData getDeleteMetaData() {
        return deleteMetaData;
    }

    public void addTableMetaData(String tableName, String aliasName) {
        boolean isDuplicate = false;
        for(TableDetails tableDetails1 : tableMetaData.getTableDetailsList()) {

            if(tableDetails1.getAliasName() == null) {
                if (aliasName == null && tableDetails1.getTableName().equals(tableName)) {
                    isDuplicate = true;
                    break;
                }
            }
            else if (tableDetails1.getTableName().equals(tableName) &&
                    tableDetails1.getAliasName().equals(aliasName)) {
                isDuplicate = true;
                break;
            }
        }

        if(!isDuplicate)
            tableMetaData.getTableDetailsList().add(new TableDetails(tableName, aliasName));
    }


    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public String getSqlType() {
        return sqlType;
    }

    public SelectMetaData getSelectMetaData() {
        return selectMetaData;
    }

    public String getSql() {
        return sql;
    }

    public TableMetaData getTableMetaData() {
        return tableMetaData;
    }

    public void setSelectMetaData(SelectMetaData selectMetaData) {
        this.selectMetaData = selectMetaData;
    }

    @Override
    public String toString() {
        return "SQLMetaData{" +
                "sqlType='" + sqlType + '\'' +
                ", sql='" + sql + '\'' +
                ", selectMetaData=" + selectMetaData +
                ", tableMetaData=" + tableMetaData +
                ", columnMetaData=" + columnMetaData +
                ", insertMetaData=" + insertMetaData +
                ", updateMetaData=" + updateMetaData +
                ", deleteMetaData=" + deleteMetaData +
                ", mergeMetaData=" + mergeMetaData +
                ", processingLeftExpression=" + processingLeftExpression +
                '}';
    }
}
