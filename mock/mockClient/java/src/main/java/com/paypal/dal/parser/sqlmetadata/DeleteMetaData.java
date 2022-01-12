package com.paypal.dal.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class DeleteMetaData {
    private List<WhereBindInMeta> whereBindInMetaList = new ArrayList<>();

    public List<WhereBindInMeta> getWhereBindInMetaList() {
        return whereBindInMetaList;
    }

    @Override
    public String toString() {
        return "DeleteMetaData{" +
                "whereBindInMetaList=" + whereBindInMetaList +
                '}';
    }
}
