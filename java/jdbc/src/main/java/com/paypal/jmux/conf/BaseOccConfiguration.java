package com.paypal.jmux.conf;

import java.util.Properties;

import com.paypal.jmux.ex.OccConfigException;

public class BaseOccConfiguration {

	protected final Properties config;
	
	public BaseOccConfiguration(Properties props) {
		this.config = props;
	}
	protected final Integer validateAndReturnDefaultInt(String pr, int min, int max, int defaultValue) throws OccConfigException {
		String sval = config.getProperty(pr);
		int ival = defaultValue;
		if (sval != null) {
			ival = Integer.parseInt(sval);
		}

		if( ival < min )
			throw new OccConfigException("OCC configuration value for property, " + pr + " cannot be less than " + min);
		if( ival > max )
			throw new OccConfigException("OCC configuration value for property, " + pr + " cannot be greater than " + max);
		return ival;
	}
	
	protected final Boolean validateAndReturnDefaultBoolean(String pr, boolean defaultValue) throws OccConfigException {
		String sval = config.getProperty(pr);
		
		if (sval == null) {
			return defaultValue;
		}

		return Boolean.valueOf(sval);
	}

	public final String validateAndReturnDefaultString(String pr, String defaultValue) throws OccConfigException {
		
		String sval = config.getProperty(pr);
			
		return (sval != null ? sval : defaultValue);
		
	}

}
