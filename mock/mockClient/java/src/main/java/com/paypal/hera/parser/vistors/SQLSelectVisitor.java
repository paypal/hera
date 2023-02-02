package com.paypal.hera.parser.vistors;

import com.paypal.hera.parser.sqlmetadata.MetaDataConstant;
import com.paypal.hera.parser.sqlmetadata.SQLMetaData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;


public class SQLSelectVisitor implements SelectVisitor {

    private SQLMetaData sqlMetaData;
    private int level;
    private String type;

    public SQLSelectVisitor (SQLMetaData sqlMetaData, int level){
        this.sqlMetaData = sqlMetaData;
        this.level = level;
        this.type = MetaDataConstant.SELECT_VISITOR;
    }

    public SQLSelectVisitor (SQLMetaData sqlMetaData, String type, int level){
        this.sqlMetaData = sqlMetaData;
        this.level = level;
        this.type = type;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        if(plainSelect.getSelectItems() != null) {
            for (SelectItem item : plainSelect.getSelectItems()) {
                item.accept(new SQLSelectItemVisitor(sqlMetaData, type, level));
            }
        }

        if(plainSelect.getFromItem() != null)
            plainSelect.getFromItem().accept(new SQLFromItemVisitor(sqlMetaData,
                    level));

        if(plainSelect.getWhere() != null)
            plainSelect.getWhere().accept(new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));

        if(plainSelect.getJoins() != null) {
            for (Join item : plainSelect.getJoins()) {
                item.getRightItem().accept(new SQLFromItemVisitor(sqlMetaData, level));
                if(item.getOnExpressions().size() > 0)
                    for(Expression expression : item.getOnExpressions())
                        expression.accept(new SQLExpressionVisitor(sqlMetaData,
                            MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
                if(item.getUsingColumns().size() > 0 || item.getJoinWindow() != null) {
                    throw new Error("Not implemented");
                }
            }
        }
        if(plainSelect.getHaving() != null) {
            plainSelect.getHaving().accept(new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
        }
    }

    @Override
    public void visit(SetOperationList setOperationList) {
        for(int i=0; i<setOperationList.getSelects().size(); i++) {
            setOperationList.getSelects().get(i).accept(this);
        }
    }

    @Override
    public void visit(WithItem withItem) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ValuesStatement valuesStatement) {
        throw new Error("Not Implemented");
    }
}
