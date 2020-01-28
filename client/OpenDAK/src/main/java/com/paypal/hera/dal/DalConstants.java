package com.paypal.hera.dal;

import java.io.File;

public interface DalConstants {
	
	public final static String DRIVER_TYPE_SQL = "sql";
	public final static String DRIVER_TYPE_SEARCH = "search";
	public final static String DRIVER_TYPE_NOSQL = "nosql";
	
	// special value for user/password values to skip user authentication
	public final static String VALUE_NONE = "none";
	
	// default encoding for US and European, besides localized encoding such as UTF-8 for Taiwan	
	String DEFAULT_ENCODING = "Cp1252";

	// encoding constants
	String UTF8_ENCODING = "UTF-8";
	String ISO_8859_1_ENCODING = "ISO-8859-1";
	
	String DAL_CONFIG_FILE_DIR = "config" + File.separator + "kerneldal";
}
