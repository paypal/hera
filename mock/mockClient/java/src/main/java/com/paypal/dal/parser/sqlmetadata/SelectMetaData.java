package com.paypal.dal.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class SelectMetaData {
    private List<SelectItemMetaData> selectItemMetaDataList = new ArrayList<>();
    private List<WhereBindInMeta> whereBindInMetaList = new ArrayList<>();

    public List<SelectItemMetaData> getSelectItemMetaDataList() {
        return selectItemMetaDataList;
    }

    public void setSelectItemMetaDataList(List<SelectItemMetaData> selectItemMetaDataList) {
        this.selectItemMetaDataList = selectItemMetaDataList;
    }

    public List<WhereBindInMeta> getWhereBindInMetaList() {
        return whereBindInMetaList;
    }


    @Override
    public String toString() {
        String msg = "SelectMetaData{";
        for (SelectItemMetaData metaData : selectItemMetaDataList)
            msg += metaData.toString();

        for (WhereBindInMeta metaData : whereBindInMetaList)
            msg += metaData.toString();

        msg += "}";
        return msg;
    }
}
