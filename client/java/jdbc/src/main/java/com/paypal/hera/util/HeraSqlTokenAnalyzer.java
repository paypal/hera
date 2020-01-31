package com.paypal.hera.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class parses the query, and looks for hera position binding token, i.e.
 * :p1,:p2,:p3,... and find their actual corresponding parameter names/columns.
 * Return the hera position binding token to actual corresponding parameter name
 * mappings, e.g. for the input sql:
 * select c.car_id, c.make, c.model,c.year from car c where c.car_id=:p1 
 * the mappings would be:
 * p1--&lt;car_id
 * 
 * Please note, the output mapping could be a subset of all the available hera position
 * binding token mappings. The parsing would ignore some mapping cases of which 
 * are impossible to be sharding key bindings. e.g. greater than etc.
 * 
 * 
 * @author towu
 *
 */
public class HeraSqlTokenAnalyzer {
	final static Logger LOGGER = LoggerFactory.getLogger(HeraSqlTokenAnalyzer.class);
	
	public static final int MAX_BIND_NAME_LEN = 30;

	@SuppressWarnings("serial")
	private static final Set<String> reserved = new HashSet<String>() {{
		add("rowid");
	}};
	
	@SuppressWarnings("serial")
	private static final List<String> oracleFunctions = new ArrayList<String>() {{
		add("substrb");
		add("substrc");
		add("substr2");
		add("substr4");
		add("substr");
		add("round");
		add("cast");
		add("to_char");
		add("to_number");
		add("abs");
		add("max");
		add("min");
		add("avg");
		add("sum");
	}};

	/**
	 * This token analysis phase takes care of parsing 'between' clause, for instance, 
	 * select c.car_id, c.make, c.model,c.year from car c where c.car_id between :p1 and :p2
	 * it would take the sql and append parsing result to the same
	 * passed in map, e.g. in above example, p1-&lt;car_id, p2-&lt;car_id2
	 * 
	 * @param sql Query text
	 * @param mappings Of binds
	 */
	protected static void parseSQLBetween(String sql, Map<String, String> mappings){

		Pattern pattern = Pattern.compile(("[\\s\\(][\\da-z.d_-]+\\s*between\\s*:p\\d+\\s*and\\s*:p\\d+\\s*"));
		Matcher matcher = pattern.matcher(sql);
		String token;
		String pName;
		String [] positions;
		while (matcher.find()) {
			token = matcher.group();
			LOGGER.debug("parseSQLBetween: " + token);
			String [] kv = token.split(" between ");

			if (kv.length != 2) {
				LOGGER.warn("parseSQLBetween, invalid between clause " + token);
				continue;
			}

			pName = getNormalizedParamName( kv[0]);

			positions = kv[1].split(" and ");

			if (positions.length != 2) {
				LOGGER.warn("parseSQLBetween, invalid between/and clause " + token);
				continue;
			}
			
			addPxToActualParamNameWithDupHandling (getNormalizedMappingKey(positions[0]), pName, mappings);
			addPxToActualParamNameWithDupHandling (getNormalizedMappingKey(positions[1]), pName, mappings);	
		}

	}

	/**
	 * This token analysis phase takes care of parsing name=value pair,
	 * for instance, "select id, name, price from product where id = :p1"
	 * it would take the sql and append parsing result to the same
	 * passed in map.
	 * 
	 * @param sql to be parsed
	 * @param mappings query binds
	 */
	protected static void parseSQLKVBinding(String sql, Map<String, String> mappings){

		Pattern pattern = Pattern.compile("\\s*[\\da-z.d_-]+\\s*\\=\\s*:p\\d+");
		Matcher matcher = pattern.matcher(sql);
		String token;
		String pIndex;
		while (matcher.find()) {
			token = matcher.group();
			LOGGER.debug("parseSQLKVBinding for token: " + token);
			String [] kv = token.split("=");

			if (kv.length != 2) {
				LOGGER.warn("parseSQLKVBinding, invalid '=' caluse " + token);
				continue;
			}

			pIndex = kv[1].trim();

			String pName = getNormalizedParamName(kv[0]);
			addPxToActualParamNameWithDupHandling (pIndex, pName, mappings);

		}
	}

	/**
	 * This token analysis phase takes care of parsing insert statement, for instance, 
	 * "insert into exp (id, name ) values (:p1, :p2 )"
	 * it would take the sql and append parsing result to the same
	 * passed in map, e.g. in above example, p1-&lt;id, p2-&lt;name
	 * 
	 * @param sql query needing parsing
	 * @param mappings binds
	 */
	protected static void parseSQLValues(String sql, Map<String, String> mappings){

		Pattern pattern = Pattern.compile("insert\\s*(into){0,1}\\s*.*\\s*\\((.*),{0,}\\)\\s*values\\s*\\(\\s*(.*)[:p\\d+,{0,}\\s*]+(.*)\\)*");
		
		Matcher matcher = pattern.matcher(sql);
		String token;
		while (matcher.find()) {
			token = matcher.group();
			LOGGER.debug("parseSQLValues for token: " + token);

			String [] kv = token.split("\\)\\s*values\\s*\\(");

			if (kv.length != 2) {
				LOGGER.warn("parseSQLValues, invalid 'values' clause " + token);
				continue;
			}
			
			// get the raw columns for the insert statement
			String rawColumns = kv[0].trim();
			if (!rawColumns.startsWith("insert")){
				LOGGER.warn("parseSQLValues, invalid insert clause " + token);
				continue;
			}
			rawColumns = rawColumns.substring(rawColumns.indexOf("(")+1);
			String [] columns = rawColumns.trim().split(",");
			if (columns.length == 0 ) {
				LOGGER.warn("parseSQLValues, no column found in the insert clause " + token);
				continue;
			}
			
			//extract hera px parameters from the 'values' part from the insert statement
			String rawValues = "(" + kv[1].trim();
			Pair<Integer, Integer> pair = getParenthesisOpenCloseIndex(rawValues);
			if (pair == null) {
				LOGGER.warn("unmatched parenthesise: " + rawValues);
				break;
			}
			int open = pair.getFirst();
			int close = pair.getSecond();
			if (open >=0  && close>0 && close>open){
				rawValues = rawValues.substring(open+1, close);
			} else {
				LOGGER.warn("exception when matching parenthesise: " + rawValues);
				continue;
			}
			Pattern p = Pattern.compile("(:p\\d+)"); 
			Matcher m = p.matcher(rawValues);
			List<String> valueTokens = new ArrayList<String>();
			while (m.find()) {
				token = m.group();
				valueTokens.add(m.group());
			}
			if (valueTokens.isEmpty()){
				// no a single hera named position param (e.g. :p1) found
				continue;
			}

			// insert columns and values across check
			if (columns.length > valueTokens.size() ) {
				rawValues = getNormalizedInsertValues(rawValues);
				//size not match, try to find constant value bindings
				String [] positions = rawValues.split(",");
				if (columns.length == positions.length ) {
					valueTokens = Arrays.asList(positions);	
				} else {
					//should never get here
					LOGGER.warn("More named param found, names.length:" + columns.length + ", values:" + rawValues);
					continue;
				}
			} else if (columns.length < valueTokens.size()) {
				//could have additional clause after insert such as " returning.. into :p5", do nothing
				LOGGER.debug("parseSQLValues, more hera position param found:" + token);
			}

			for (int i=0; i<columns.length;i++){
				try {
					if (!valueTokens.get(i).trim().startsWith(":p")){
						//could be value instead of param binding e.g. 123.3
						LOGGER.debug("parseSQLValues slipping: " + valueTokens.get(i));
						continue;
					}
					String mappingKey = getNormalizedMappingKey(valueTokens.get(i));
					if (isBindingNameValid(mappingKey) && 
							isHeraPositionParamValid(valueTokens.get(i).trim())) {
						addPxToActualParamNameWithDupHandling (mappingKey, columns[i], mappings);
					}
				} catch (Exception e) {
					LOGGER.error("ignoring " + valueTokens.get(i).trim() + " to " + columns[i].trim() + " mapping:" + e);
					continue;
				}
			}
		}

	}

	/**
	 * This token analysis phase takes care of parsing 'in' clause, for instance, 
	 * select c.car_id, c.make, c.model,c.year from car c where c.car_id in (:p1, :p2) 
	 * it would take the sql and append parsing result to the same
	 * passed in map, e.g. in above example, p1-&lt;car_id, p2-&lt;car_id2
	 * 
	 * @param sql Parsable query
	 * @param mappings Bind map
	 */
	protected static void parseSQLIn(String sql, Map<String, String> mappings){

		Pattern pattern = Pattern.compile("\\s*[\\da-z.d_-]+\\s*in\\s*\\(\\s*(:p\\d+\\s*,{0,1}\\s*)+\\)*");
		Matcher matcher = pattern.matcher(sql);
		String token;
		String pName;
		while (matcher.find()) {
			token = matcher.group();

			String [] kv = token.split("\\s+in\\s+\\(");

			if (kv.length != 2) {
				//System.out.println("skip sql for IN clause matching");
				continue;
			}

			kv[1] = kv[1].replaceAll("\\(", "").replaceAll("\\)", "");

			pName =  getNormalizedParamName( kv[0]);
			
			if (pName.equalsIgnoreCase("not")){
				//ignore "not in" clause
				continue;
			}
			
			String [] positions = kv[1].trim().split(",");

			for (int i=0; i<positions.length;i++){

				if (!positions[i].trim().startsWith(":p")){
					//could be value instead of param binding e.g.  123.3
					continue;
				}

				addPxToActualParamNameWithDupHandling (positions[i], pName, mappings);
			}	
		}
	}

	/**
	 * This is public entry point API for the Token Analyzer. It performs multiple phases 
	 * hera token parsing and analysis. It returns a map that contains the hera position 
	 * binding token to its actual param name binding, for example:
	 * "select id, name, price from product where id = :p1"
	 * the output binding would be: "p1" ==&lt; "id"
	 * 
	 * Note:
	 * The returned map could contain only subset of all hera parameter names 
	 * in a hera sql because some hera params are not possible to be used as 
	 * sharding key, e.g. open end "&lt;=" parameter. Token Analyszer would ignore 
	 * parsing those params for sake of efficiency and complexity. 
	 * 
	 * The actual param names in the returned map don't have table alias 
	 * prefix.
	 * 
	 * @param sql Query to parse
	 * @return Bind name mapping
	 */
	public static Map<String, String> getHeraParamToActualParamNameBindings (String sql){

		if (sql == null || sql.isEmpty()){
			return Collections.emptyMap();
		}

		sql = sql.toLowerCase().trim();
		if (! isSQLShardingSupported(sql) ){
			LOGGER.debug("unsupported sql for sharding: " + sql);
			return Collections.emptyMap();
		}

		Map<String, String> mappings = new LinkedHashMap<String, String>();
		LOGGER.debug("Token Analysis for: " + sql);
		
		try {
			parseSQLKVBinding(sql, mappings);
			parseSQLValues(sql, mappings);
			parseSQLBetween(sql, mappings);
			parseSQLIn(sql, mappings);
		} catch (Exception e) {
			LOGGER.error("Hera Token Analysis exception: " + sql + ", " + e);
		}

		LOGGER.debug("Token Analysis output: " + mappings);

		return mappings;
	}

	private static boolean isSQLShardingSupported(String sql) {

		// is this PL/SQL
		if (sql.contains("begin") && sql.contains("end") 
				&& sql.contains(";")){
			LOGGER.debug("Name binding does not support PL/SQL:" + sql);
			return false;
		}

		// is this stored procedure
		if (sql.contains("call ") && sql.contains("{")  && sql.contains("}")){
			LOGGER.debug("Name binding does not support stored procedure" + sql);
			return false;
		}

		return true;
	}

	/**
	 * This method does two things 1) trim off table alias with dot. 
	 * 2) remove parenthesis.
	 * @param rawParam
	 * @return
	 */
	private static String getNormalizedParamName(String rawParam){
		rawParam = rawParam.replaceAll("\\(", "").replaceAll("\\)", "");
		String [] params = rawParam.trim().split("\\.");
		return params.length==2?params[1]:params[0];
	}
	
	private static String getNormalizedMappingKey(String key){
		
		String res = key.trim();
		
		if (res.endsWith(")")){
			res = res.substring(0, res.length()-1);
		}
		return res.trim();
	}
	
	/**
	 * This method scans each insert values, normalizes a value if there is 
	 * hera position binding within a Oracle function call 
	 * @param insertValues
	 * @return
	 */
	private static String getNormalizedInsertValues(String insertValues){

		String result = insertValues;

		int i = 0;
		while (i< oracleFunctions.size()) {

			String fn = oracleFunctions.get(i);
			if (!result.contains(fn)) {
				i++;
				continue;
			}

			StringBuffer sb = new StringBuffer();
			int idx = result.indexOf(fn);
			sb.append(result.substring(0, idx));

			String rest = result.substring(idx);

			Pair<Integer, Integer> pair = getParenthesisOpenCloseIndex(rest);
			if (pair == null) {
				LOGGER.warn("unmatched parenthesise: " + rest);
				break;
			}

			int open = pair.getFirst();
			int close = pair.getSecond();

			if (open >0  && close>0 && close>open){
				sb.append("functionbindingblock").append(rest.substring(close+1));
				result = sb.toString();
			} else {
				LOGGER.warn("exception when append functionbindingblock: " + rest);
				break;
			}
		}
		
		return result.replaceAll("\\(", "").replaceAll("\\)", "");
	}
	
	/**
	 * Parse the String and get the first parenthesize and its matching closing parenthesize
	 * Position index.
	 * 
	 * This is to handle nested Parenthesizes, e.g.
	 * "round((to_date(to_char(sys_extract_utc(systimestamp),'yyyymmddhh24miss'),'yyyymmddhh24miss') - to_date ('19700101000000','yyyymmddhh24miss')) * 86400), round((to_date(to_char(sys_extract_utc(systimestamp),'yyyymmddhh24miss'),'yyyymmddhh24miss') - to_date ('19700101000000','yyyymmddhh24miss')) * 86400)"
	 * 
	 * @param v
	 * @return
	 */
	private static Pair<Integer, Integer> getParenthesisOpenCloseIndex(String v){

		int open = v.indexOf("(");
		if (open < 0 ) {
			return null;
		}

		Stack<Character> stack = new Stack<Character>();

		int close = -1;
		char c;
		for(int i=0; i < v.length(); i++) {
			c = v.charAt(i);

			if(c == '(') {
				stack.push(c);

			}else if(c == ')'){
				if(stack.empty()) {
					//error
					return null;

				}else if(stack.peek() == '('){
					stack.pop();

					if (stack.isEmpty()) {
						//found then close 
						close = i;
						break;
					}
				}
			}
		}

		if (close<=open) {
			return null;
		}

		return new Pair<Integer, Integer>(open, close);
	}

	private static boolean isBindingNameValid(String rawBindingName){

		// reserved word binding would cause hera server error
		if (reserved.contains(rawBindingName)){
			return false;
		}
		
		//this would be part of express, e.g. inventory / 12,
		//exclude since this would never be sharding key binding
		if (Character.isDigit(rawBindingName.charAt(0))){
			return false;
		}

		//should not be a statement like
		if (rawBindingName.contains(" ") || rawBindingName.contains(",")){
			return false;
		}

		//should not have two hera name position, e.g. :p3.. :p5
		if (rawBindingName.lastIndexOf(":p") != rawBindingName.indexOf(":p")) {
			return false;
		}
		return true;
	}
	
	private static boolean isHeraPositionParamValid(String heraPosParam){
		
		Pattern p3 = Pattern.compile(":p\\d+");
		Matcher m3 = p3.matcher(heraPosParam);
		if (m3.find()) {
			return true;
		}
		return false;
	}

	private static boolean isWithinHeraParamLengthLimit (String param){
		return param.length() > MAX_BIND_NAME_LEN? false:true;
	}

	/**
	 * This method adds the hera position token (with prefix ':' stripped off) 
	 * to actual param name mapping. If hera position token already exists, 
	 * it still add it but with incremental index suffix.
	 * 
	 * @param pIndex hera position token e.g. :p1, :p2
	 * @param pName actual param name mapping e.g. entity_id
	 * @param mappings
	 */
	private static void addPxToActualParamNameWithDupHandling (
			String pIndex, String pName, Map<String, String> mappings){
		
		pName = getNormalizedParamName(pName);

		if (!isBindingNameValid(pName) ){
			return;
		}
		
		if (!isHeraPositionParamValid(pIndex.trim())){
			return;
		}

		Collection<String> set = mappings.values();
		if (set.contains(pName)) {
			// So the same param exist already,
			// still add it but with incremental index
			int end = 2;
			while (set.contains(pName + end)) {
				end++;
			}
			if (isWithinHeraParamLengthLimit(pName + end)){
				mappings.put(pIndex.trim().substring(1), pName + end);
			}
		} else {
			if (isWithinHeraParamLengthLimit(pName)){
				mappings.put(pIndex.trim().substring(1), pName);
			}
		}
	}


}
