package com.paypal.dal.parser.vistors;

import com.paypal.dal.parser.sqlmetadata.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.List;
import java.util.Locale;

public class SQLExpressionVisitor implements ExpressionVisitor {

    private String type;
    private SQLMetaData sqlMetaData;
    private List<WhereBindInMeta> whereBindInMetaList;
    private List<SelectItemMetaData> selectItemMetaDataList;
    private List<InsertItemMetaData> insertItemMetaDataList;
    private List<InsertItemMetaData> updateItemMetaDataList;
    private boolean selectItem = false;
    private boolean whereBind = false;
    private boolean insertBind = false;
    private boolean updateBind = false;
    private int level;
    private int itemListIndex = -1;
    private boolean selectColumnAdded = false;

    private void init() {
        if (this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_SELECT_ITEM))
            selectItem = true;
        else if (this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS))
            whereBind = true;
        else if (this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_INSERT_BINDS))
            insertBind = true;
        else if (this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_UPDATE_BINDS) ||
                this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_UPDATE_SELECT_BINDS))
            updateBind = true;
        else if (this.type.equals(MetaDataConstant.EXPRESSION_VISITOR_DELETE_BINDS))
            whereBind = true;
    }

    public void setSelectColumnAdded(boolean selectColumnAdded) {
        this.selectColumnAdded = selectColumnAdded;
    }

    public SQLExpressionVisitor(SQLMetaData sqlMetaData,
                                String type, int level, int itemListIndex){
        this(sqlMetaData, type, level);
        if(!type.equals(MetaDataConstant.INSERT_SELECT_VISITOR))
            this.itemListIndex = itemListIndex;
    }

    public SQLExpressionVisitor(SQLMetaData sqlMetaData,
                                String type, int level) {
        this.sqlMetaData = sqlMetaData;
        this.whereBindInMetaList = sqlMetaData.getSelectMetaData().getWhereBindInMetaList();
        this.selectItemMetaDataList = sqlMetaData.getSelectMetaData().getSelectItemMetaDataList();
        this.insertItemMetaDataList = sqlMetaData.getInsertMetaData().getInsertItemMetaDataList();
        this.updateItemMetaDataList = sqlMetaData.getUpdateMetaData().getUpdateItemMetaDataList();

        if(sqlMetaData.getSqlType().equals(MetaDataConstant.UPDATE)) {
            this.whereBindInMetaList = sqlMetaData.getUpdateMetaData().getWhereBindInMetaList();
        } else if (sqlMetaData.getSqlType().equals(MetaDataConstant.DELETE)) {
            this.whereBindInMetaList = sqlMetaData.getDeleteMetaData().getWhereBindInMetaList();
        } else if (sqlMetaData.getSqlType().equals(MetaDataConstant.MERGE)) {
            this.whereBindInMetaList = sqlMetaData.getMergeMetaData().getWhereBindInMetaList();
            this.insertItemMetaDataList = sqlMetaData.getMergeMetaData().getInsertItemMetaDataList();
            this.updateItemMetaDataList = sqlMetaData.getMergeMetaData().getUpdateItemMetaDataList();
        } else if (sqlMetaData.getSqlType().equals(MetaDataConstant.INSERT)) {
            this.whereBindInMetaList = sqlMetaData.getInsertMetaData().getSelectMetaData().getWhereBindInMetaList();
            this.selectItemMetaDataList = sqlMetaData.getInsertMetaData().getSelectMetaData().getSelectItemMetaDataList();
        }

        this.type = type;
        this.level = level;
        init();
    }

    @Override
    public void visit(BitwiseRightShift bitwiseRightShift) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseLeftShift bitwiseLeftShift) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(NullValue nullValue) {
        // nothing to do
    }

    @Override
    public void visit(Function function) {
        if(function.getParameters() != null && !selectItem)
            function.getParameters().accept(new SQLItemListVisitor(sqlMetaData, type, level));
        else if(function.getMultipartName().size() == 1 && !selectItem) {
            if(updateBind && type.equals(MetaDataConstant.EXPRESSION_VISITOR_UPDATE_SELECT_BINDS)) {
                InsertItemMetaData insertItemMetaData = updateItemMetaDataList.get(updateItemMetaDataList.size()-1);
                ConstantValue constantValue = new ConstantValue();
                constantValue.setValue(function.getMultipartName().get(0));
                constantValue.setType(LongValue.class.getSimpleName());
                insertItemMetaData.setConstantValue(constantValue);
            } else {
                SelectItemMetaData selectItemMetaData = new SelectItemMetaData(null, null, null,
                        level, null);
                ConstantValue constantValue = new ConstantValue();
                constantValue.setValue(function.getMultipartName().get(0));
                constantValue.setType(LongValue.class.getSimpleName());
                selectItemMetaData.setConstantValue(constantValue);
                sqlMetaData.getSelectMetaData().getSelectItemMetaDataList().add(selectItemMetaData);
            }
        } else if(!selectItem) {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        if(whereBind) {
            WhereBindInMeta whereBindInMeta;
            if(sqlMetaData.isProcessingLeftExpression())
                whereBindInMetaList.add(new WhereBindInMeta());
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
            whereBindInMeta.getVariableName().add("p" + jdbcParameter.getIndex());
        } else if(updateBind) {
            InsertItemMetaData insertItemMetaData = updateItemMetaDataList.get(updateItemMetaDataList.size()-1);
            insertItemMetaData.setVariableName("p" + jdbcParameter.getIndex());
        } else if(insertBind || this.type.equals(MetaDataConstant.INSERT_SELECT_VISITOR)) {
            InsertItemMetaData insertItemMetaData;
            if(this.type.equals(MetaDataConstant.INSERT_SELECT_VISITOR))
                insertItemMetaDataList.add(new InsertItemMetaData(null, null));
            if (itemListIndex == -1) {
                insertItemMetaData = insertItemMetaDataList.get(insertItemMetaDataList.size()-1);
            } else {
                insertItemMetaData = insertItemMetaDataList.get(itemListIndex);
            }
            insertItemMetaData.setVariableName("p" + jdbcParameter.getIndex());
        } else {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        if(whereBind) {
            WhereBindInMeta whereBindInMeta;
            if(sqlMetaData.isProcessingLeftExpression())
                whereBindInMetaList.add(new WhereBindInMeta());
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
            whereBindInMeta.getVariableName().add(jdbcNamedParameter.getName());
        } else if(insertBind || this.type.equals(MetaDataConstant.INSERT_SELECT_VISITOR)) {
            InsertItemMetaData insertItemMetaData;
            if(this.type.equals(MetaDataConstant.INSERT_SELECT_VISITOR))
                insertItemMetaDataList.add(new InsertItemMetaData(null, null));

            if(type.equals(MetaDataConstant.EXPRESSION_VISITOR_INSERT_BINDS) &&
                    sqlMetaData.getInsertMetaData().isColumnNotSpecified()) {
                insertItemMetaDataList.add(new InsertItemMetaData(null, null));
                insertItemMetaData = insertItemMetaDataList.get(insertItemMetaDataList.size()-1);
            }
            else if (itemListIndex == -1) {
                insertItemMetaData = insertItemMetaDataList.get(insertItemMetaDataList.size()-1);
            } else {
                insertItemMetaData = insertItemMetaDataList.get(itemListIndex);
            }
            insertItemMetaData.setVariableName(jdbcNamedParameter.getName());
        } else if(updateBind) {
            if(type.equals(MetaDataConstant.EXPRESSION_VISITOR_UPDATE_SELECT_BINDS)) {
                InsertItemMetaData insertItemMetaData = new InsertItemMetaData(null, jdbcNamedParameter.getName());
                insertItemMetaData.setVariableName(jdbcNamedParameter.getName());
                updateItemMetaDataList.add(insertItemMetaData);
            } else {
                InsertItemMetaData insertItemMetaData = updateItemMetaDataList.get(updateItemMetaDataList.size() - 1);
                insertItemMetaData.setVariableName(jdbcNamedParameter.getName());
            }
        } else if(sqlMetaData.getSqlType().equals("Merge")) {
            if(selectItem) {
                WhereBindInMeta whereBindInMeta;
                if(sqlMetaData.isProcessingLeftExpression())
                    whereBindInMetaList.add(new WhereBindInMeta());
                whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
                whereBindInMeta.getVariableName().add(jdbcNamedParameter.getName());
            } else {
                throw new Error("not implemented");
            }
        }
        else if(selectItem) {
            if(!this.selectColumnAdded) {
                selectItemMetaDataList.add(new SelectItemMetaData(jdbcNamedParameter.getName(), null,
                        null, level, null));
            }
            WhereBindInMeta whereBindInMeta = new WhereBindInMeta();
            whereBindInMeta.getVariableName().add(jdbcNamedParameter.getName());
            whereBindInMetaList.add(whereBindInMeta);
        }
        else {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        if(whereBind) {
            WhereBindInMeta whereBindInMeta;
            if(sqlMetaData.isProcessingLeftExpression())
                whereBindInMetaList.add(new WhereBindInMeta());
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(String.valueOf(doubleValue.getValue()));
            constantValue.setType(LongValue.class.getSimpleName());
            whereBindInMeta.setConstantValue(constantValue);
        } else {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(LongValue longValue) {
        if(whereBind) {
            WhereBindInMeta whereBindInMeta;
            if(sqlMetaData.isProcessingLeftExpression())
                whereBindInMetaList.add(new WhereBindInMeta());
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(longValue.getStringValue());
            constantValue.setType(LongValue.class.getSimpleName());
            whereBindInMeta.setConstantValue(constantValue);
        } else if (updateBind){
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(longValue.getStringValue());
            constantValue.setType(LongValue.class.getSimpleName());
            InsertItemMetaData insertItemMetaData = updateItemMetaDataList.get(updateItemMetaDataList.size()-1);
            insertItemMetaData.setConstantValue(constantValue);
        } else if(insertBind || this.type.equals(MetaDataConstant.INSERT_SELECT_VISITOR)) {
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(longValue.getStringValue());
            constantValue.setType(LongValue.class.getSimpleName());
            InsertItemMetaData insertItemMetaData;
            if(sqlMetaData.getInsertMetaData().isColumnNotSpecified())
                insertItemMetaDataList.add(new InsertItemMetaData(null, null));
            if (itemListIndex == -1) {
                insertItemMetaData = insertItemMetaDataList.get(insertItemMetaDataList.size() - 1);
            } else {
                insertItemMetaData = insertItemMetaDataList.get(itemListIndex);
            }

            insertItemMetaData.setConstantValue(constantValue);
        } else if (!selectItem) {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(HexValue hexValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        if(whereBind || updateBind) {
            WhereBindInMeta whereBindInMeta;
            if(sqlMetaData.isProcessingLeftExpression())
                whereBindInMetaList.add(new WhereBindInMeta());
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(stringValue.getValue());
            constantValue.setType(LongValue.class.getSimpleName());
            whereBindInMeta.setConstantValue(constantValue);
        } else if (selectItem) {
            SelectItemMetaData selectItemMetaData = new SelectItemMetaData(null,
                    null, null, level, null);
            ConstantValue constantValue = new ConstantValue();
            constantValue.setValue(stringValue.getValue());
            constantValue.setType(LongValue.class.getSimpleName());
            selectItemMetaData.setConstantValue(constantValue);
            sqlMetaData.getSelectMetaData().getSelectItemMetaDataList().add(selectItemMetaData);
        } else if (insertBind){
            return;
        } else {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        addition.getRightExpression().accept(this);
    }

    @Override
    public void visit(Division division) {
        sqlMetaData.setProcessingLeftExpression(true);
        division.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        division.getRightExpression().accept(this);
    }

    @Override
    public void visit(IntegerDivision integerDivision) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        multiplication.getRightExpression().accept(this);
    }

    @Override
    public void visit(Subtraction subtraction) {
        subtraction.getLeftExpression().accept(this);
        subtraction.getRightExpression().accept(this);
    }

    @Override
    public void visit(AndExpression andExpression) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        andExpression.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        andExpression.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(OrExpression orExpression) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        orExpression.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        orExpression.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(XorExpression xorExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Between between) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        between.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        equalsTo.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        equalsTo.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        greaterThan.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        greaterThan.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        greaterThanEquals.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        greaterThanEquals.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(InExpression inExpression) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        if( inExpression.getLeftExpression() != null) {
            inExpression.getLeftExpression().accept(this);
        } else if (inExpression.getMultiExpressionList() != null) {
            throw new Error("Not Implemented");
        }
        sqlMetaData.setProcessingLeftExpression(false);
        if (inExpression.getRightExpression() != null) {
            inExpression.getRightExpression().accept(this);
        }
        if(inExpression.getRightItemsList() != null) {
            if(inExpression.getRightItemsList() instanceof SubSelect) {
                WhereBindInMeta whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size()-1);
                whereBindInMeta.getVariableName().add("SubSelect");
            }
            inExpression.getRightItemsList().accept(new SQLItemListVisitor(sqlMetaData, type, level));
        }
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        isNullExpression.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        sqlMetaData.setProcessingLeftExpression(true);
        likeExpression.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        likeExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThan minorThan) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        minorThan.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        minorThan.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        boolean oldValue = sqlMetaData.isProcessingLeftExpression();
        sqlMetaData.setProcessingLeftExpression(true);
        minorThanEquals.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        minorThanEquals.getRightExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(oldValue);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        sqlMetaData.setProcessingLeftExpression(true);
        notEqualsTo.getLeftExpression().accept(this);
        sqlMetaData.setProcessingLeftExpression(false);
        notEqualsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        if(selectItem) {
            String tableName = null;
            if (column.getTable() != null)
                tableName = (column.getTable().getName());

            if(!selectColumnAdded) {
                SelectItemMetaData selectItemMetaData = new SelectItemMetaData(column.getColumnName(), tableName,
                        column.getColumnName(), level, null);
                selectItemMetaDataList.add(selectItemMetaData);
            }
        } else if (whereBind || (updateBind && !this.type.equals("EXPRESSION_VISITOR_UPDATE_SELECT_BINDS"))) {
            WhereBindInMeta whereBindInMeta;
            if (sqlMetaData.isProcessingLeftExpression()) {
                whereBindInMetaList.add(new WhereBindInMeta());
            }
            whereBindInMeta = whereBindInMetaList.get(whereBindInMetaList.size() - 1);
            whereBindInMeta.setColumnName(column.getColumnName());
            if (column.getTable() != null)
                whereBindInMeta.setTableName(column.getTable().getName());
        } else if  (updateBind  && this.type.equals("EXPRESSION_VISITOR_UPDATE_SELECT_BINDS")) {
            String tableName = null;
            if(column.getTable() != null)
                tableName = column.getTable().getName();
            selectItemMetaDataList.add(new SelectItemMetaData(
                    column.getColumnName(), tableName, column.getColumnName(), level, null));
        } else if (this.sqlMetaData.getSqlType().equals("Merge") && insertBind){
            insertItemMetaDataList.get(insertItemMetaDataList.size() - 1).setBindIn(false);
            insertItemMetaDataList.get(insertItemMetaDataList.size() - 1).setVariableName(column.getColumnName());

            if(column.getTable() != null)
                insertItemMetaDataList.get(insertItemMetaDataList.size()-1).setTableName(column.getTable().getName());
        } else if (!(insertBind &&
                (column.getColumnName().toUpperCase(Locale.ROOT).endsWith("NEXTVAL") ||
                        column.getColumnName().toUpperCase(Locale.ROOT).equals("SYSTIMESTAMP") ||
                        column.getColumnName().toUpperCase(Locale.ROOT).equals("SYSDATE") ||
                        column.getColumnName().toUpperCase(Locale.ROOT).equals("SESSIONTIMEZONE")))) {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        subSelect.getSelectBody().accept(new SQLSelectVisitor(sqlMetaData, type, level+1));
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        for(int i=0; i<caseExpression.getWhenClauses().size(); i++) {
            caseExpression.getWhenClauses().get(i).accept(new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
        }
        if(caseExpression.getElseExpression() != null)
            caseExpression.getElseExpression().accept(new SQLExpressionVisitor(sqlMetaData,
                    MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
    }

    @Override
    public void visit(WhenClause whenClause) {
        whenClause.getWhenExpression().accept(new SQLExpressionVisitor(sqlMetaData,
                MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
        whenClause.getThenExpression().accept(new SQLExpressionVisitor(sqlMetaData,
                MetaDataConstant.EXPRESSION_VISITOR_WHERE_BINDS, level));
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        if(existsExpression.getRightExpression() != null)
            existsExpression.getRightExpression().accept(this);
        else
            throw new Error("Not Implemented");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Concat concat) {
        concat.getLeftExpression().accept(this);
        concat.getRightExpression().accept(this);
    }

    @Override
    public void visit(Matches matches) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        bitwiseOr.getLeftExpression().accept(this);
        bitwiseOr.getRightExpression().accept(this);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CastExpression castExpression) {
        castExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {
        if (analyticExpression.getExpression() != null)
            analyticExpression.getExpression().accept(this);
    }

    @Override
    public void visit(ExtractExpression extractExpression) {
        extractExpression.getExpression().accept(this);
    }

    @Override
    public void visit(IntervalExpression intervalExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(RegExpMatchOperator regExpMatchOperator) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JsonExpression jsonExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JsonOperator jsonOperator) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(UserVariable userVariable) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(NumericBind numericBind) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(KeepExpression keepExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(MySQLGroupConcat mySQLGroupConcat) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ValueListExpression valueListExpression) {
        valueListExpression.getExpressionList().accept(new SQLItemListVisitor(sqlMetaData, type, level));
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(RowGetExpression rowGetExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(OracleHint oracleHint) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        if (!(selectItem||whereBind)) {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {
        if (!selectItem) {
            throw new Error("Not Implemented");
        }
    }

    @Override
    public void visit(NotExpression notExpression) {
        notExpression.getExpression().accept(this);
    }

    @Override
    public void visit(NextValExpression nextValExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CollateExpression collateExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(SimilarToExpression similarToExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ArrayExpression arrayExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ArrayConstructor arrayConstructor) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(VariableAssignment variableAssignment) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(XMLSerializeExpr xmlSerializeExpr) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(TimezoneExpression timezoneExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JsonAggregateFunction jsonAggregateFunction) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JsonFunction jsonFunction) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ConnectByRootOperator connectByRootOperator) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(OracleNamedFunctionParameter oracleNamedFunctionParameter) {
        throw new Error("Not Implemented");
    }
}
