package com.paypal.dal.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class UpdateMetaData {
    private List<WhereBindInMeta> whereBindInMetaList = new ArrayList<>();
    private List<InsertItemMetaData> updateItemMetaDataList = new ArrayList<>();

    public List<InsertItemMetaData> getUpdateItemMetaDataList() {
        return updateItemMetaDataList;
    }

    public List<WhereBindInMeta> getWhereBindInMetaList() {
        return whereBindInMetaList;
    }

    @Override
    public String toString() {
        return "UpdateMetaData{" +
                "whereBindInMetaList=" + whereBindInMetaList +
                ", insertItemMetaDataList=" + updateItemMetaDataList +
                '}';
    }
}
