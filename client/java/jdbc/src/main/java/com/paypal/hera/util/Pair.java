package com.paypal.hera.util;

public class Pair<T1, T2> {
	private T1 first;
	private T2 second;
	
	public void setFirst(T1 _first) {
		first = _first;
	}

	public void setSecond(T2 _second) {
		second = _second;
	}

	public Pair(T1 _first, T2 _second) {
		first = _first;
		second = _second;
	}
	
	public T1 getFirst() {
		return first;
	}
	
	public T2 getSecond() {
		return second;
	}
}
