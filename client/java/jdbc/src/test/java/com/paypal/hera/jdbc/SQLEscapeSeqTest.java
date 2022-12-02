package com.paypal.hera.jdbc;

import java.lang.reflect.Method;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.client.HeraClientImpl;
import com.paypal.hera.conf.HeraClientConfigHolder.E_DATASOURCE_TYPE;
import com.paypal.hera.util.HeraStatementsCache;
import com.paypal.hera.util.HeraStatementsCache.StatementCacheEntry;

public class SQLEscapeSeqTest {
	static final Logger LOGGER = LoggerFactory.getLogger(HeraClientImpl.class);
	
	public String preprocessEscapeCall(String input, E_DATASOURCE_TYPE type) {
		HeraStatementsCache heraStmt = new HeraStatementsCache(0, "");
		StatementCacheEntry stmtEntry = heraStmt.new StatementCacheEntry(input, true,
				false, false, type, HeraDriver.getQueryProperties());
		String output = null;
		try {
			Method method = StatementCacheEntry.class.getDeclaredMethod("preprocessEscape", String.class, E_DATASOURCE_TYPE.class);
			method.setAccessible(true);
			output = (String) method.invoke(stmtEntry, input, type);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output;
	}

	@Test
	public void test_oracle_sqlEscaping(){
		String sql = "{ call DALCERT_INSERT_EMPLOYEE() }";
		String outputExpected = "BEGIN DALCERT_INSERT_EMPLOYEE() ; END;";
		Assert.assertTrue("Oracle escape seq SQL ", preprocessEscapeCall(sql, E_DATASOURCE_TYPE.ORACLE).equals(outputExpected));
		
	}
	
	@Test
	public void test_mysql_sqlEscaping(){
		String sql = "{ call DALCERT_INSERT_EMPLOYEE() }";
		String outputExpected = "CALL DALCERT_INSERT_EMPLOYEE() ;";
		Assert.assertTrue("MySql escape seq SQL ", preprocessEscapeCall(sql, E_DATASOURCE_TYPE.MYSQL).equals(outputExpected));
	}

}
