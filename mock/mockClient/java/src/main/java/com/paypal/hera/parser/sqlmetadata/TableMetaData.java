package com.paypal.hera.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;


public class TableMetaData {

    List<TableDetails> tableDetailsList = new ArrayList<>();

    public List<TableDetails> getTableDetailsList() {
        return tableDetailsList;
    }

    public void setTableDetailsList(List<TableDetails> tableDetailsList) {
        this.tableDetailsList = tableDetailsList;
    }

    @Override
    public String toString() {
        String msg = "";
        for(TableDetails tableDetails: tableDetailsList)
            msg += tableDetails.toString();
        return msg;
    }
}
