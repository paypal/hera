package com.paypal.hera.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.paypal.hera.cal.ClsLogOutputHelper;
import com.paypal.hera.jdbc.HeraDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.paypal.hera.cal.CalTransaction;
import com.paypal.hera.cal.CalTransactionFactory;
import com.paypal.hera.conf.HeraClientConfigHolder.E_DATASOURCE_TYPE;

public class HeraStatementsCache {
	public enum StatementType {
		UNKNOWN,
		DML,
		NON_DML
	};

	public class ShardingInfo {
		public String sk;
		public ArrayList<Integer> skPos;
		public ArrayList<Integer> scuttle_idPos;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(HeraStatementsCache.class);
	static final Pattern escapePattern = Pattern.compile("[\\s]*\\{[\\s]*call[\\s]*(.*)[\\s]*\\}[\\s]*");
	static final Pattern shardingHintPattern = Pattern.compile(".*/\\* HERASK=(.+)\\(([\\d,]+)\\),ScuttleId\\(([\\d,]+)\\) \\*/.*", Pattern.DOTALL);

	public class StatementCacheEntry {
		private String parsedSQL;
		private ArrayList<HeraColumnMeta> columnMeta;
		private HashMap<String, Integer> columnIndexes;
		private int paramCount;
		private StatementType statementType;
		private ShardingInfo shardingInfo = null;
		private Map<String, String> paramPosToNameMap;
		private boolean sqlEligibleForCache = true;
		private boolean paramNameBindingEnabled = false;
		private byte[] sqlByteArray= null;
		private String lastStmtId;


		private int timeoutInMilliSecond = -1;

		public int getParamCount() {
			return paramCount;
		}

		public ShardingInfo getShardingInfo() {
			return shardingInfo;
		}

		public Map<String, String> getParamPosToNameMap() {
			return paramPosToNameMap;
		}

		public int getTimeoutInMilliSecond() {
			return timeoutInMilliSecond;
		}

		public boolean isSqlEligibleForCache() {
			return sqlEligibleForCache;
		}

		public StatementCacheEntry(String _sql, boolean _escapeProcessingEnabled,
								   boolean _shardingEnabled, boolean _paramNameBindingEnabled,
								   E_DATASOURCE_TYPE datasource, Map<String, Integer> queryTimeoutConfigs) {
			parsedSQL = helperParseSQL(_sql, _escapeProcessingEnabled,
					_shardingEnabled, _paramNameBindingEnabled, datasource, queryTimeoutConfigs);
			lastStmtId = Long.toString(ClsLogOutputHelper.writeSQLStmt(parsedSQL));
			paramNameBindingEnabled = _paramNameBindingEnabled;
			setStatementType(StatementType.UNKNOWN);
			sqlByteArray = parsedSQL.getBytes();
		}

		public final String actualParamName(int _index) {

			String paraName = null;

			if (_index < PARAM_CNT_CACHE)
				paraName =  PAR_NAMES[_index];
			else
				paraName = buildParamName(_index);

			if ( !paramNameBindingEnabled || paramPosToNameMap == null
					|| paramPosToNameMap.isEmpty()) {
				return paraName;
			}

			String actualName = paramPosToNameMap.get (paraName);
			return actualName==null?paraName:actualName.trim();
		}


		/***
		 *  1> replace "?" with ":p1", ":p2"...
		 *  2> bypass the comment inside the sql. in case that the comment or the begin or end of comment
		 *  is inside the a string value, the method will not change that value.
		 *
		 *  3> find the total param count
		 */
		private String helperParseSQL(String _sql, boolean _escapeProcessingEnabled,
									  boolean _shardingEnabled, boolean _paramNameBindingEnabled,
									  E_DATASOURCE_TYPE datasource,
									  Map<String, Integer> queryTimeOutConfig) {
			if (_sql == null) {
				throw new NullPointerException("SQL string is null");
			}
			if (_escapeProcessingEnabled) {
				_sql = preprocessEscape(_sql, datasource);
			}
			if (_shardingEnabled) {
				parseShardingHint(_sql);
			}
			// TODO: optimize it
			final int DEF_MAX_PARAM_CNT = 5;
			int len = _sql.length();
			StringBuffer sb = new StringBuffer(len + DEF_MAX_PARAM_CNT * 4);
			StringBuffer sbCommentsOnly = new StringBuffer(len );
			int i=0;

			/*** scan the entire sql statement */
			while (i < len) {
				// run into the begin of the comment
				if (_sql.charAt(i) =='/' && i < (len -1) && _sql.charAt(i+1) == '*') {
					/* find the end of the comment */
					int endComment =  _sql.indexOf("*/", i+2);
					if (endComment == -1) {
						// no end comment, bad sql just ignore it. How about the /* was in a literal : select x from t where attr='abcd/*xyz' ***/
						sb.append("/*"); // not process the unpaired begin comment and move on
						sbCommentsOnly.append("/*");
						i +=2;
					} else {
						/* got a comment, un-process it */
						sb.append(_sql.substring(i, endComment +2));
						sbCommentsOnly.append(_sql.substring(i, endComment +2));
						i = endComment +2;
					}
				} else if (_sql.charAt(i)== '?') {
					paramCount++;
					sb.append(":").append(paramName(paramCount));
					i++;
				} else {
					/* any other char */
					sb.append(_sql.charAt(i));
					i++;
				}
			}

			sqlEligibleForCache = shouldCacheSQL(sbCommentsOnly.toString());

			timeoutInMilliSecond = getQueryTimeOut(sbCommentsOnly.toString(), queryTimeOutConfig);

			if (_paramNameBindingEnabled) {

				// get the hera param position to actual param name mappings, e.g. "p1"->"columnname1"
				paramPosToNameMap = HeraQueryParamNameBindingCache.
						getInstance().getNameBindings(sb.toString());

				//replace param position with param name, e.g. ":p1" with ":columnname1" etc.
				return preprocessParamNames(sb.toString());
			}

			return sb.toString();
		}

		private String preprocessParamNames(String _sql) {
			LOGGER.debug("Preprocess param names for: " + _sql);

			for (String key: paramPosToNameMap.keySet()){
				if (_sql.contains(":" + key)){
					_sql = _sql.replaceFirst(":" + key, ":"+paramPosToNameMap.get(key).trim());
				} else {
					//should never get here
					LOGGER.warn("error in replacement for key:" + key + " for sql:" + _sql);
				}
			}

			return _sql;
		}

		private boolean shouldCacheSQL(String _sqlCommentsOnly) {
			// if sql comment has DisableStmtCache, then disable cache, by default cache is enabled
			if(_sqlCommentsOnly != null)
			{
				return !_sqlCommentsOnly.contains("DisableStmtCache");
			}
			return true;
		}

		private String preprocessEscape(String _sql, E_DATASOURCE_TYPE datasource) {
			LOGGER.debug("Preprocess escape for: " + _sql);
			Matcher m = escapePattern.matcher(_sql);
			if (m.find()) {
				if(datasource == E_DATASOURCE_TYPE.ORACLE) {
					_sql = "BEGIN " +  m.group(1) + "; END;" ;

				} else {
					_sql = "CALL " + m.group(1) + ";";
				}
				LOGGER.debug("Found call escape, SQL is: " + _sql);
			}
			return _sql;
		}

		private void parseShardingHint(String _sql) {
			if(_sql != null && _sql.contains("HERASK=")) {
				CalTransaction prepareCalTxn = CalTransactionFactory.create("HERAJDBC");
				prepareCalTxn.setName("HERASK");
				prepareCalTxn.setStatus("0");

				LOGGER.debug("parseShardingHint for: " + _sql);
				Matcher m = shardingHintPattern.matcher(_sql);
				if (m.find() && (m.groupCount() == 3)) {
					LOGGER.debug("Shard key: " + m.group(1));
					LOGGER.debug("Shard key pos: " + m.group(2));
					LOGGER.debug("Scuttle_id pos: " + m.group(3));
					shardingInfo = new ShardingInfo();
					shardingInfo.sk = m.group(1);
					// Java regex doesn't work with repetitive groups, so we need to parse the positions with split()
					String[] pos = m.group(2).split(",");
					shardingInfo.skPos = new ArrayList<Integer>();
					for (String s : pos) {
						shardingInfo.skPos.add(Integer.parseInt(s));
					}
					pos = m.group(3).split(",");
					shardingInfo.scuttle_idPos = new ArrayList<Integer>();
					for (String s : pos) {
						shardingInfo.scuttle_idPos.add(Integer.parseInt(s));
					}
				}
				prepareCalTxn.completed();
			}
		}

		public String getParsedSQL() {
			return parsedSQL;
		}
		public byte[] getParsedSqlByte(){
			return sqlByteArray;
		}
		public String getLastStmtId() {
			return lastStmtId;
		}
		public ArrayList<HeraColumnMeta> getColumnMeta() {
			return columnMeta;
		}
		public void setColumnMeta(ArrayList<HeraColumnMeta> columnMeta) {
			this.columnMeta = columnMeta;
		}
		public HashMap<String, Integer> getColumnIndexes() {
			if ((columnIndexes == null) && (columnMeta != null)) {
				columnIndexes = new HashMap<String, Integer>();
				Iterator<HeraColumnMeta> it = columnMeta.iterator();
				Integer index = 1;
				while (it.hasNext()) {
					HeraColumnMeta meta = it.next();
					columnIndexes.put(meta.getName().toUpperCase(), index);
					index++;
				}
			}
			return columnIndexes;
		}
		public StatementType getStatementType() {
			return statementType;
		}

		public final void setStatementType(StatementType statementType) {
			this.statementType = statementType;
		}

		private int getQueryTimeOut(String _sqlCommentsOnly, Map<String, Integer> queryTimeOutConfig) {
			int timeout = -1;
			String qName = "UNKNOWN";
			for (String key : queryTimeOutConfig.keySet()) {
				if (_sqlCommentsOnly.contains("/* " + key + " */") ||
						_sqlCommentsOnly.contains("/* " + key + "*/") ||
						_sqlCommentsOnly.contains("/*" + key + " */") ||
						_sqlCommentsOnly.contains("/*" + key + "*/")) {
					timeout = queryTimeOutConfig.get(key);
					qName = key;
					break;
				}
			}
			if (timeout > 0) {
				LOGGER.debug("Found MilliSec level timeout for " + qName + " as " + timeout);
			}
			return timeout;
		}
	}

	private final static String PAR_PREFIX = "p";
	private final static int PARAM_CNT_CACHE = 100;
	private final static String[] PAR_NAMES = new String[PARAM_CNT_CACHE];

	private Object lock = new Object();

	private BoundLRUCaches<StatementCacheEntry> stmtCache;

	public HeraStatementsCache(int _size, String _key) {
		stmtCache = new BoundLRUCaches<StatementCacheEntry>(_size, _key);
	}

	private static final String buildParamName(int _index) {
		return PAR_PREFIX + _index;
	}
	private static void init() {

		for (int i = 0; i < PAR_NAMES.length; i++)
			PAR_NAMES[i] = buildParamName(i);
	}

	static {
		init();
	}

	public static final String paramName(int _index) {
		if (_index < PARAM_CNT_CACHE)
			return PAR_NAMES[_index];
		else
			return buildParamName(_index);
	}


	/// parse the SQL statement transforming ? into parameter names
	public StatementCacheEntry getEntry(String _sql, boolean _escapeProcessingEnabled,
										boolean _shardingEnabled, boolean _paramNameBindingEnabled, E_DATASOURCE_TYPE datasource) {
		StatementCacheEntry entry = stmtCache.get(_sql);
		if (entry == null) {
			synchronized (lock) {
				entry = new StatementCacheEntry(_sql, _escapeProcessingEnabled,
						_shardingEnabled, _paramNameBindingEnabled, datasource, HeraDriver.getQueryProperties());
				if(entry.isSqlEligibleForCache()) {
					stmtCache.put(_sql, entry);
				}
			}
		}
		return entry;
	}
}
