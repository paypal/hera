package com.paypal.hera.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeraStatementsCachePool {
	private final static int STMT_CACHE_SIZE = 1000;

	static Map<String, HeraStatementsCache> s_caches = new ConcurrentHashMap<String, HeraStatementsCache>();

	public static HeraStatementsCache getStatementsCache(String url) {
		if (s_caches.containsKey(url)){
			return s_caches.get(url);
		}
		return new HeraStatementsCache(STMT_CACHE_SIZE, url);
	}

	public static void releaseStatementsCache(String url, HeraStatementsCache stCache) {
		if (!s_caches.containsKey(url)){
			s_caches.put(url, stCache);
		}
	}

}
