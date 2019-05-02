package com.paypal.jmux.cal;

public class CalStreamUtils {
	static CalStreamUtils instance = new CalStreamUtils();
	public static CalStreamUtils getInstance() {
		// TODO Auto-generated method stub
		return instance;
	}
	public CalStream getDefaultCalStream() {
		// TODO Auto-generated method stub
		return CalStream.Instance;
	}

}
