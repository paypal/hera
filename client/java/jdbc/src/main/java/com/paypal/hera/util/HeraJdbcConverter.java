package com.paypal.hera.util;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.paypal.hera.ex.HeraRuntimeException;
import com.paypal.hera.jdbc.HeraBlob;
import com.paypal.hera.jdbc.HeraClob;

// influenced by all/ifeature/utility/database/dao/Converter.cpp
public class HeraJdbcConverter {
	// v1. to be removed after the hera rollout
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("dd-MMM-yy");
	private SimpleDateFormat dateFormatterTZ = new SimpleDateFormat("dd-MMM-yy");
	private SimpleDateFormat timestampFormatterRead = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.S    a");
	private SimpleDateFormat timestampFormatterWrite = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSS a");
	private SimpleDateFormat timestampFormatterWriteTZ = new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSS a");
	// v2
	private SimpleDateFormat dateTimeFormatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
	private SimpleDateFormat dateTimeFormatterTZDef = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
	private SimpleDateFormat dateTimeFormatterTZ = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS XXX");

	//static byte[] MIN_INT_VALUE = "-2147483648".getBytes();
	static byte[] MIN_LONG_VALUE = "-9223372036854775808".getBytes();
		
	public HeraJdbcConverter() {
		
	}
	
	public static int hera2int(byte[] bytes) {
		try {
			return Integer.parseInt(new String(bytes, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}
	
	// like String InfraCharsetUtil::detect_and_convert ( const Buffer &_src )
	// try UTF8, fallback to latin-1
	public static String hera2String(byte[] bytes) {
		try {
			return new String(bytes, "UTF8");
		} catch (UnsupportedEncodingException e) {
			try {
				return new String(bytes, "Cp1252"); // Windows Latin-1: http://docs.oracle.com/javase/1.5.0/docs/guide/intl/encoding.doc.html
			} catch (UnsupportedEncodingException e1) {
				return new String("");
			}
		}
	}

	public static long hera2long(byte[] bs) {
		try {
			return Long.parseLong(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	public static float hera2float(byte[] bs) {
		try {
			return Float.parseFloat(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	public static double hera2double(byte[] bs) {
		try {
			return Double.parseDouble(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	public static short hera2short(byte[] bs) {
	   try {
		return Short.parseShort(new String(bs, "UTF-8"));
	   } catch (UnsupportedEncodingException e) {
		   throw new NumberFormatException();
	   }
	}

	@SuppressWarnings("deprecation")
	public Date hera2date(byte[] bs) throws ParseException {
		Date date = new Date(dateTimeFormatter.parse(hera2String(bs)).getTime());
		return new Date(date.getYear(), date.getMonth(), date.getDate());
	}

	public Time hera2time(byte[] bs) throws ParseException {
		return new Time(dateTimeFormatter.parse(hera2String(bs)).getTime());
	}

	public Timestamp hera2timestamp(byte[] bs) throws ParseException {
		return new Timestamp(dateTimeFormatter.parse(hera2String(bs)).getTime());
	}
		
	public Timestamp hera2timestamp(byte[] bs, Calendar c) throws ParseException, SQLFeatureNotSupportedException {
		if (bs.length == 23) {
			if (c != null) {
				dateTimeFormatterTZDef.setTimeZone(c.getTimeZone());
			} else {
				dateTimeFormatterTZDef.setTimeZone(Calendar.getInstance().getTimeZone());
			}
			return new Timestamp(dateTimeFormatterTZDef.parse(hera2String(bs)).getTime());
		} else if (bs.length == 30) {
			return new Timestamp(dateTimeFormatterTZ.parse(hera2String(bs)).getTime());
		}
		else {
			throw new ParseException("String length is not of a timestamp: ", bs.length);
		}
	}
		
	public static byte[] int2hera(int x) {
		return Integer.toString(x).getBytes();
	}

	public static byte[] short2hera(short x) {
		return Short.toString(x).getBytes();
	}

	public static byte[] long2hera(long x) {
		return Long.toString(x).getBytes();
	}

	public static int int2hera(int x, byte[] out) {
		return long2hera(x, out);
	}

	public static int short2hera(short x, byte[] out) {
		return long2hera(x, out);
	}
	
	public static int long2hera(long x, byte[] out) {
		boolean negative = false;
		if (x < 0) {
			if (x == Long.MIN_VALUE) {
				System.arraycopy(MIN_LONG_VALUE, 0, out, 0, MIN_LONG_VALUE.length);
				return MIN_LONG_VALUE.length;
			}
			negative = true;
			x = x * (-1);			
		}
		int pos = out.length - 1;
		do {
			out[pos--] = (byte)((x % 10) + '0');
			x /= 10;
		} while (x != 0);
		if (negative) {
			out[pos--] = '-';
		}
		return pos + 1;
	}

	public static byte[] float2hera(float x) {
		return Float.toString(x).getBytes();
	}

	public static byte[] double2hera(double x) {
		return Double.toString(x).getBytes();
	}

	public static byte[] string2hera(String str) throws UnsupportedEncodingException {
		return str.getBytes("UTF8");
	}

	private byte[] helperDateTime2hera(java.util.Date x) throws UnsupportedEncodingException {
		return string2hera(dateTimeFormatter.format(x));
	}
	
	private byte[] helperDateTime2hera(java.util.Date x, Calendar c) throws UnsupportedEncodingException {
		if (c != null) {
			c.setTime(x);
			dateTimeFormatterTZ.setTimeZone(c.getTimeZone());
		} else {
			dateTimeFormatterTZ.setTimeZone(Calendar.getInstance().getTimeZone());
		}
		return string2hera(dateTimeFormatterTZ.format(x));
	}
	
	@SuppressWarnings("deprecation")
	public byte[] date2hera(Date x) throws UnsupportedEncodingException {
		return helperDateTime2hera(new Date(x.getYear(), x.getMonth(), x.getDate()));
	}

	@SuppressWarnings("deprecation")
	public byte[] date2hera(Date x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2hera(new Date(x.getYear(), x.getMonth(), x.getDate()), c);
	}

	public byte[] time2hera(Time x) throws UnsupportedEncodingException {
		return helperDateTime2hera(x);
	}

	public byte[] time2hera(Time x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2hera(x, c);
	}

	public byte[] timestamp2hera(Timestamp x) throws UnsupportedEncodingException {
		return helperDateTime2hera(x);
	}
	
	public byte[] timestamp2hera(Timestamp x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2hera(x, c);
	}	
	
	public static Blob hera2Blob(byte[] bs) {
		return new HeraBlob(bs);
	}

	public static Clob hera2Clob(byte[] bs) {
		return new HeraClob(bs);
	}

	public static byte[] bigDecimal2hera(BigDecimal x) {
		return x.toPlainString().getBytes();
	}

	public static BigDecimal hera2BigDecimal(byte[] bs) {
		try {
			return new BigDecimal(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	private static byte hex2int(byte data) throws HeraRuntimeException {
		if ((data >= '0') && (data <= '9'))
			return (byte) (data - '0');
		if ((data >= 'A') && (data <= 'F'))
			return (byte) (data - 'A' + 10);
		if ((data >= 'a') && (data <= 'f'))
			return (byte) (data - 'a' + 10);
		throw new HeraRuntimeException("Invalid hex digit: " + data);
	}

	public static byte[] hex2Binary(byte[] data) throws HeraRuntimeException {
		byte[] ret = new byte[data.length / 2];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = (byte) (hex2int(data[i * 2]) * 16 + hex2int(data[i * 2 + 1]));
		}
		return ret;
	}

	// TODO implement the other conversion functions
}
