package com.paypal.hera.cal;

public class CalClientConfigMXBeanImpl {
	static CalClientConfigMXBeanImpl instance = new CalClientConfigMXBeanImpl();
	public static CalClientConfigMXBeanImpl getInstance() {
		return instance;
	}
	// TODO: implement this
	public String getPoolname() {
		return "jmux";
	}

}
