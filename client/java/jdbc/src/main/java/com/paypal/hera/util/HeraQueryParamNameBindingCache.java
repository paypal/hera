package com.paypal.hera.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeraQueryParamNameBindingCache {

	private static HeraQueryParamNameBindingCache instance = new HeraQueryParamNameBindingCache();
	private Map<String, Map<String, String>> cache = new ConcurrentHashMap<String, Map<String,String>>();

	private HeraQueryParamNameBindingCache(){
	}

	public static HeraQueryParamNameBindingCache getInstance(){
		return instance;
	}

	public Map<String, String> getNameBindings(String sql){

		if (cache.containsKey(sql)){
			return cache.get(sql);
		} 
		Map<String, String> entry =  HeraSqlTokenAnalyzer.getHeraParamToActualParamNameBindings(sql);
		cache.put(sql, entry);

		return entry;
	}
	
	protected Map<String, Map<String, String>> getCache() {
		return cache;
	}

}
