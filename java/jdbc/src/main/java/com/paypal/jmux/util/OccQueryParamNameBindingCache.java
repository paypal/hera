package com.paypal.jmux.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OccQueryParamNameBindingCache {

	private static OccQueryParamNameBindingCache instance = new OccQueryParamNameBindingCache();
	private Map<String, Map<String, String>> cache = new ConcurrentHashMap<String, Map<String,String>>();

	private OccQueryParamNameBindingCache(){
	}

	public static OccQueryParamNameBindingCache getInstance(){
		return instance;
	}

	public Map<String, String> getNameBindings(String sql){

		if (cache.containsKey(sql)){
			return cache.get(sql);
		} 
		Map<String, String> entry =  OccSqlTokenAnalyzer.getOccParamToActualParamNameBindings(sql);
		cache.put(sql, entry);

		return entry;
	}
	
	protected Map<String, Map<String, String>> getCache() {
		return cache;
	}

}
