package com.paypal.dal.parser;

import com.paypal.dal.parser.sqlmetadata.SQLMetaData;
import com.paypal.dal.parser.vistors.SQLStatementVisitor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class Parser {

    public SQLMetaData sqlMetaData;

    public void parse(String sql) throws Exception{
        sqlMetaData = new SQLMetaData(sql);
        if(sql.contains("AND ((1 = 1)) "))
            sql = sql.replace("AND ((1 = 1)) ", "");
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            SQLStatementVisitor visitor = new SQLStatementVisitor(sqlMetaData, 0);
            stmt.accept(visitor);
        } catch (JSQLParserException parseException) {
            System.out.println("SQL :" + sql + "\n" + parseException.getMessage());
        }
    }
}
