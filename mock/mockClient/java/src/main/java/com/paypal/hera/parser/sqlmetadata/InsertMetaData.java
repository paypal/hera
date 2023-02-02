package com.paypal.hera.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class InsertMetaData {
    private List<InsertItemMetaData> insertItemMetaDataList = new ArrayList<>();
    private SelectMetaData selectMetaData = new SelectMetaData();
    private boolean columnNotSpecified = false;

    public List<InsertItemMetaData> getInsertItemMetaDataList() {
        return insertItemMetaDataList;
    }

    public SelectMetaData getSelectMetaData() {
        return selectMetaData;
    }

    public void setColumnNotSpecified(boolean columnNotSpecified) {
        this.columnNotSpecified = columnNotSpecified;
    }

    public boolean isColumnNotSpecified() {
        return columnNotSpecified;
    }

    @Override
    public String toString() {
        return "InsertMetaData{" +
                "insertItemMetaDataList=" + insertItemMetaDataList +
                ", selectMetaData=" + selectMetaData +
                ", columnNotSpecified=" + columnNotSpecified +
                '}';
    }
}
