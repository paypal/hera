package com.paypal.dal.parser.vistors;

import com.paypal.dal.parser.sqlmetadata.BindMeta;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SQLWhereExpressionVisitor implements ExpressionVisitor, ItemsListVisitor {
    public List<BindMeta> binds = new ArrayList<>();
    int moveNext = 0;
    boolean startExpression = true;
    Map<String, String> tableAlias;
    Map<String, String> columnAlias;
    Set<String> tableNames;
    String tableNameWithOutAlias;
    boolean isOracleSql;

    private void moveNext(){
        moveNext += 1;
    }

    private void movePrevious() {
        moveNext -= 1;
    }

    private boolean shouldMoveToNext() {
        return moveNext == 0 && startExpression;
    }

    private void checkAndMoveToNextBindIn() {
        return;
//        if(shouldMoveToNext()) {
//            index += 1;
//            binds.add(new BindMeta(isOracleSql));
//            binds.get(index).setColumnName("UnKnown");
//        }
    }

    public void init(Map<String, String> tableAlias, Map<String, String> columnAlias,
                     Set<String> tableNames, boolean isOracleSql) {
        this.tableAlias = tableAlias;
        this.tableNames = tableNames;
        this.columnAlias = columnAlias;
        this.isOracleSql = isOracleSql;
        for(String tableName : tableNames) {
            boolean foundAlias = false;
            for (String alias : tableAlias.keySet()) {
                if ((isOracleSql && tableAlias.get(alias).equalsIgnoreCase(tableName)) ||
                        tableAlias.get(alias).equals(tableName)) {
                    foundAlias = true;
                    break;
                }
            }

            if(!foundAlias && tableNameWithOutAlias == null)
                tableNameWithOutAlias = tableName;
            else if(!foundAlias)
                throw new Error ("" +
                        "There are two tables without alias " + tableNameWithOutAlias + " and " + tableName +
                        "\n Found Alias: " + tableAlias);
        }
    }

    int index = -1;

    @Override
    public void visit(NullValue nullValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseRightShift var1){
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseLeftShift var1) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Function function) {
        function.getParameters().accept(this);
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        checkAndMoveToNextBindIn();
        binds.get(index).addBindValue(jdbcNamedParameter.getName());
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(LongValue longValue) {
        checkAndMoveToNextBindIn();
        binds.get(index).addBindConstant(String.valueOf(longValue.getValue()));
    }

    @Override
    public void visit(HexValue hexValue) {
        checkAndMoveToNextBindIn();
        binds.get(index).addBindConstant(hexValue.getValue());
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
        checkAndMoveToNextBindIn();
        binds.get(index).addBindConstant(stringValue.getValue());
    }

    @Override
    public void visit(Addition addition) {
        addition.getLeftExpression().accept(this);
        startExpression = false;
        addition.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(Division division) {
        division.getLeftExpression().accept(this);
        startExpression = false;
        division.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(IntegerDivision integerDivision) {
        integerDivision.getLeftExpression().accept(this);
        startExpression = false;
        integerDivision.getLeftExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(Multiplication multiplication) {
        multiplication.getLeftExpression().accept(this);
        startExpression = false;
        multiplication.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(Subtraction subtraction) {
        subtraction.getLeftExpression().accept(this);
        startExpression = false;
        subtraction.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        startExpression = false;
        andExpression.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(OrExpression orExpression) {
        orExpression.getLeftExpression().accept(this);
        startExpression = false;
        orExpression.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(XorExpression xorExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Between between) {
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        startExpression = false;
        between.getBetweenExpressionEnd().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        equalsTo.getLeftExpression().accept(this);
        startExpression = false;
        equalsTo.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        greaterThan.getLeftExpression().accept(this);
        startExpression = false;
        greaterThan.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        greaterThanEquals.getLeftExpression().accept(this);
        startExpression = false;
        greaterThanEquals.getRightExpression().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(InExpression inExpression) {
        binds.add(new BindMeta(isOracleSql));
        binds.get(index+1).setInClause(true);

        inExpression.getLeftExpression().accept(this);
        startExpression = false;
        inExpression.getRightItemsList().accept(this);
        startExpression = true;
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        isNullExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        startExpression = true;
        isBooleanExpression.getLeftExpression().accept(this);
        startExpression = false;
        isBooleanExpression.accept(this);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(MinorThan minorThan) {
        minorThan.getLeftExpression().accept(this);
        minorThan.getRightExpression().accept(this);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        minorThanEquals.getLeftExpression().accept(this);
        minorThanEquals.getRightExpression().accept(this);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        notEqualsTo.getLeftExpression().accept(this);
        notEqualsTo.getRightExpression().accept(this);
    }

    @Override
    public void visit(Column column) {
        index += 1;
        if(binds.size() == index+1 && !binds.get(index).isInClause())
            binds.add(new BindMeta(isOracleSql));
        else if(binds.size() != index+1)
            binds.add(new BindMeta(isOracleSql));
        binds.get(index).setColumnName(column.getColumnName());
        String columnName = column.getColumnName();
        if(columnAlias.containsKey(columnName)) {
            columnName = columnAlias.get(columnName);
        }

        if (!columnName.equalsIgnoreCase("ROWNUM") && !columnName.equalsIgnoreCase("ROWCOUNT")) {
            if(column.getTable() == null && tableNameWithOutAlias != null) {
                binds.get(index).setTableName(tableNameWithOutAlias);
            } else if(column.getTable() != null) {
                String tab = column.getTable().getName();
                if (tableAlias.containsKey(tab))
                    tab = tableAlias.get(tab);
                binds.get(index).setTableName(tab);
            } else if(tableAlias.size() == 1) {
                binds.get(index).setTableName(tableAlias.get(tableAlias.keySet().toArray()[0]));
            } else{
                throw new Error("unable to find table name for column " +
                        columnName + " table " + column.getTable() + " tableNameWithOutAlias " +
                        tableNameWithOutAlias);
            }
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        PlainSelect plainSelect = ((PlainSelect) (subSelect).getSelectBody());
        plainSelect.getWhere().accept(this);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        List<Expression> expressions = expressionList.getExpressions();
        for(Expression expression :expressions)
            expression.accept(this);
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(MultiExpressionList multiExpressionList) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new Error("Not Implemented");
    }


    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Concat concat) {
        throw new Error("Not Implemented");
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
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(CastExpression castExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(Modulo modulo) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(AnalyticExpression analyticExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(ExtractExpression extractExpression) {
        throw new Error("Not Implemented");
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
        throw new Error("Not Implemented");
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
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {
        throw new Error("Not Implemented");
    }

    @Override
    public void visit(NotExpression notExpression) {
        throw new Error("Not Implemented");
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
