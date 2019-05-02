package com.paypal.hera.constants;

public enum BindType {
	HERA_TYPE_STRING(0),
	HERA_TYPE_BLOB(1),
	HERA_TYPE_CLOB(2),
	HERA_TYPE_RAW(3),
	HERA_TYPE_BLOB_SINGLE_ROUND(4),
	HERA_TYPE_CLOB_SINGLE_ROUND(5),
	HERA_TYPE_TIMESTAMP(6),
	HERA_TYPE_TIMESTAMP_TZ(7);
	
	private final int value;
	
	BindType(int _value) {
		value = _value;
	}

	public int getValue() {
		return value;
	}
}
