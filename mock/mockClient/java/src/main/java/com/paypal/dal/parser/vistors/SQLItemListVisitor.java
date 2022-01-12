package com.paypal.dal.parser.vistors;

import com.paypal.dal.parser.sqlmetadata.SQLMetaData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.statement.select.SubSelect;

public class SQLItemListVisitor implements ItemsListVisitor {

    private SQLMetaData sqlMetaData;

    private String type;
    private int level;

    public SQLItemListVisitor(SQLMetaData sqlMetaData, String type, int level){
        this.sqlMetaData = sqlMetaData;

        this.level = level;
        this.type = type;
    }

    @Override
    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(new SQLSelectVisitor(sqlMetaData, level+1));
    }

    @Override
    public void visit(ExpressionList expressionList) {
        for(int i=0; i<expressionList.getExpressions().size(); i++) {
            Expression list = expressionList.getExpressions().get(i);
            list.accept(new SQLExpressionVisitor(sqlMetaData,
                    type, level, i));
        }
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(MultiExpressionList multiExpressionList) {
        throw new Error("Not Implemented");
    }
}
