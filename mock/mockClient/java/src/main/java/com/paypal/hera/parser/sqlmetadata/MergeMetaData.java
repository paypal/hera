package com.paypal.hera.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class MergeMetaData {
    private List<WhereBindInMeta> whereBindInMetaList = new ArrayList<>();
    private List<InsertItemMetaData> updateItemMetaDataList = new ArrayList<>();
    private List<InsertItemMetaData> insertItemMetaDataList = new ArrayList<>();

    public List<WhereBindInMeta> getWhereBindInMetaList() {
        return whereBindInMetaList;
    }

    public List<InsertItemMetaData> getInsertItemMetaDataList() {
        return insertItemMetaDataList;
    }

    public List<InsertItemMetaData> getUpdateItemMetaDataList() {
        return updateItemMetaDataList;
    }

    @Override
    public String toString() {
        return "MergeMetaData{" +
                "whereBindInMetaList=" + whereBindInMetaList +
                ", updateItemMetaDataList=" + updateItemMetaDataList +
                ", insertItemMetaDataList=" + insertItemMetaDataList +
                '}';
    }
}
