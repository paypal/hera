package com.paypal.jmux.constants;

/* per infra/utility/database/OCCShared/OCCGlobal.h */
public enum BindType {
	OCC_TYPE_STRING(0),
	OCC_TYPE_BLOB(1),
	OCC_TYPE_CLOB(2),
	OCC_TYPE_RAW(3),
	OCC_TYPE_BLOB_SINGLE_ROUND(4),
	OCC_TYPE_CLOB_SINGLE_ROUND(5),
	OCC_TYPE_TIMESTAMP(6),
	OCC_TYPE_TIMESTAMP_TZ(7);
	
	private final int value;
	
	BindType(int _value) {
		value = _value;
	}

	public int getValue() {
		return value;
	}
}
