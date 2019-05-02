package com.paypal.jmux.util;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class BoundLRUCaches<T> extends LinkedHashMap<String, T> { 
	private static final long serialVersionUID = 6569697570036353693L;
	private int max;
	public BoundLRUCaches(int _max) {max = _max;}
	@Override
	protected boolean removeEldestEntry(Entry<String, T> eldest) {
		return (this.size() > max);
	}
}
