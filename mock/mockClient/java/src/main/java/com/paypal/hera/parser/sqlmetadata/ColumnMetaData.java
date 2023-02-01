package com.paypal.hera.parser.sqlmetadata;

import java.util.ArrayList;
import java.util.List;

public class ColumnMetaData {
    List<ColumnDetails> columnDetails = new ArrayList<>();

    public List<ColumnDetails> getColumnDetails() {
        return columnDetails;
    }

    public void setColumnDetails(List<ColumnDetails> columnDetails) {
        this.columnDetails = columnDetails;
    }

    @Override
    public String toString() {
        return "ColumnMetaData{" +
                "columnDetails=" + columnDetails +
                '}';
    }
}
