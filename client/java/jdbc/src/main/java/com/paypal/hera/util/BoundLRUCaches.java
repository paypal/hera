package com.paypal.hera.util;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class BoundLRUCaches<T> extends LinkedHashMap<String, T> {
	private static final long serialVersionUID = 6569697570036353693L;
	private int max;
	private String key;
	public BoundLRUCaches(int _max, String _key) {
		max = _max;
		key = _key;
	}
	@Override
	protected boolean removeEldestEntry(Entry<String, T> eldest) {
		return (this.size() > max);
	}
}
