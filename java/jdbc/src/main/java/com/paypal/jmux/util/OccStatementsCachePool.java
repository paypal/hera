package com.paypal.jmux.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OccStatementsCachePool {
	private final static int STMT_CACHE_SIZE = 1000;

	static Map<String, OccStatementsCache> s_caches = new ConcurrentHashMap<String, OccStatementsCache>();

	public static OccStatementsCache getStatementsCache(String url) {
		if (s_caches.containsKey(url)){
			return s_caches.get(url);
		} 
		return new OccStatementsCache(STMT_CACHE_SIZE); 
	}

	public static void releaseStatementsCache(String url, OccStatementsCache stCache) {
		if (!s_caches.containsKey(url)){
			s_caches.put(url, stCache);
		} 
	}

}
