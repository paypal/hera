package com.paypal.dal.parser.vistors;

import com.paypal.dal.parser.sqlmetadata.MetaDataConstant;
import com.paypal.dal.parser.sqlmetadata.SQLMetaData;
import com.paypal.dal.parser.sqlmetadata.TableDetails;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

public class SQLFromItemVisitor implements FromItemVisitor {

    private SQLMetaData sqlMetaData;
    private int level;

    public SQLFromItemVisitor(SQLMetaData sqlMetaData, int level) {
        this.sqlMetaData = sqlMetaData;
        this.level = level;
    }

    @Override
    public void visit(Table table) {
        String alias = null;
        if(table.getAlias() != null)
            alias = table.getAlias().getName();
        sqlMetaData.addTableMetaData(table.getName(), alias);
    }

    @Override
    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(new SQLSelectVisitor(sqlMetaData, level+1));
    }

    @Override
    public void visit(SubJoin subJoin) {
        subJoin.getLeft().accept(new SQLFromItemVisitor(sqlMetaData, level));
        for(int i=0; i<subJoin.getJoinList().size(); i++) {
            subJoin.getJoinList().get(i).getRightItem().accept(new SQLFromItemVisitor(sqlMetaData, level));
            subJoin.getJoinList().get(i).getOnExpression().accept(new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_SELECT_ITEM, level));
        }
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(TableFunction tableFunction) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ParenthesisFromItem parenthesisFromItem) {
        if (parenthesisFromItem.getFromItem() instanceof Table) {
            Table table = (Table) parenthesisFromItem.getFromItem();
            String alias = null;
            if(table.getAlias() != null)
                alias = table.getAlias().getName();
            sqlMetaData.getTableMetaData().getTableDetailsList().add(new TableDetails(table.getName(), alias));
        } else if (parenthesisFromItem.getFromItem() instanceof SubSelect) {
            ((SubSelect) parenthesisFromItem.getFromItem()).getSelectBody().accept(
                    new SQLSelectVisitor(sqlMetaData, level+1));
        }
        else {
            throw new Error("Not Implemented");
        }
    }
}
