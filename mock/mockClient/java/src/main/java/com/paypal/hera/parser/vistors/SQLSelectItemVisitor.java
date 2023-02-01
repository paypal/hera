package com.paypal.hera.parser.vistors;

import com.paypal.dal.parser.sqlmetadata.*;
import com.paypal.hera.parser.sqlmetadata.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import java.util.List;


public class SQLSelectItemVisitor implements SelectItemVisitor {
    private SQLMetaData sqlMetaData;
    private int level;
    private String type;

    public SQLSelectItemVisitor(SQLMetaData sqlMetaData, int level) {
        this.sqlMetaData = sqlMetaData;
        this.level = level;
        this.type = MetaDataConstant.SELECT_VISITOR;
    }

    public SQLSelectItemVisitor(SQLMetaData sqlMetaData, String type, int level) {
        this.sqlMetaData = sqlMetaData;
        this.level = level;
        this.type = type;
    }

    @Override
    public void visit(AllColumns allColumns) {
        SelectItemMetaData selectItemMetaData = new SelectItemMetaData(allColumns.toString(), null,
                "AllColumns", level, null);
        sqlMetaData.getSelectMetaData().getSelectItemMetaDataList().add(selectItemMetaData);
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        String tableName = null;
        if(allTableColumns.getTable() != null){
            tableName = allTableColumns.getTable().getName();
        }
        SelectItemMetaData selectItemMetaData = new SelectItemMetaData(allTableColumns.toString(), tableName,
                "AllColumns", level, null);
        sqlMetaData.getSelectMetaData().getSelectItemMetaDataList().add(selectItemMetaData);
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        if(selectExpressionItem.getAlias() != null) {
            if(sqlMetaData.getSqlType().equals("Update")) {
                sqlMetaData.getUpdateMetaData().getUpdateItemMetaDataList().add(
                        new InsertItemMetaData(null, selectExpressionItem.getAlias().getName()));
            } else {
                List<SelectItemMetaData> selectItemMetaDataList = sqlMetaData.getSelectMetaData().getSelectItemMetaDataList();
                String originalColumnName = null;
                if(selectExpressionItem.getExpression() instanceof Column) {
                    originalColumnName = ((Column) selectExpressionItem.getExpression()).getColumnName();
                }
                selectItemMetaDataList.add(new SelectItemMetaData(selectExpressionItem.getAlias().getName(), null,
                        null, level, originalColumnName));
            }
        }
        if(this.type.equals(MetaDataConstant.SELECT_VISITOR)) {
            SQLExpressionVisitor sqlExpressionVisitor = new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_SELECT_ITEM, level);
            sqlExpressionVisitor.setSelectColumnAdded(selectExpressionItem.getAlias() != null);
            selectExpressionItem.getExpression().accept(sqlExpressionVisitor);
        }
        else {
            selectExpressionItem.getExpression().accept(new SQLExpressionVisitor(sqlMetaData,
                    type, level));
        }
        if(selectExpressionItem.getExpression() instanceof Column && selectExpressionItem.getAlias() != null) {
            Column column = ((Column)selectExpressionItem.getExpression());
            String tableName = null;
            if(column.getTable() != null)
                tableName = column.getTable().getName();
            sqlMetaData.getColumnMetaData().getColumnDetails().add(new ColumnDetails(
                    tableName, column.getColumnName(), selectExpressionItem.getAlias().getName()));
        }
    }
}
