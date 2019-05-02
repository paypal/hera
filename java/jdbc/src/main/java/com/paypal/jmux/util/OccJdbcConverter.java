package com.paypal.jmux.util;

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

import com.paypal.jmux.ex.OccRuntimeException;
import com.paypal.jmux.jdbc.OccBlob;
import com.paypal.jmux.jdbc.OccClob;

// influenced by all/ifeature/utility/database/dao/Converter.cpp
public class OccJdbcConverter {
	// v1. to be removed after the occ rollout
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
		
	public OccJdbcConverter() {
		
	}
	
	public static int occ2int(byte[] bytes) {
		 try {
	            try {
					return Integer.parseInt(new String(bytes, "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					throw new NumberFormatException();
				}
	          } catch (NumberFormatException ex) {
	               double d;
				try {
					d = Double.parseDouble( new String(bytes, "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					throw new NumberFormatException();
				}
	               return (int)d;
	          }
		}
	
	// like String InfraCharsetUtil::detect_and_convert ( const Buffer &_src )
	// try UTF8, fallback to latin-1
	public static String occ2String(byte[] bytes) {
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

	public static long occ2long(byte[] bs) {
		try {
			try {
				return Long.parseLong(new String(bs, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new NumberFormatException();
			}
        } catch (NumberFormatException ex) {
             double d;
			try {
				d = Double.parseDouble( new String(bs, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new NumberFormatException();
			}
            return (long)d;
        }
	}

	public static float occ2float(byte[] bs) {
		try {
			return Float.parseFloat(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	public static double occ2double(byte[] bs) {
		try {
			return Double.parseDouble(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	public static short occ2short(byte[] bs) {
		   try {
	           try {
				return Short.parseShort(new String(bs, "UTF-8"));
	           } catch (UnsupportedEncodingException e) {
	        	   throw new NumberFormatException();
	           }
	       } catch (NumberFormatException ex) {
	           double d;
			try {
				d = Double.parseDouble( new String(bs, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new NumberFormatException();
			}
	           return (short)d;
	       }	
		}

	@SuppressWarnings("deprecation")
	public Date occ2date(byte[] bs) throws ParseException {
		Date date = new Date(dateTimeFormatter.parse(occ2String(bs)).getTime());
		return new Date(date.getYear(), date.getMonth(), date.getDate());
	}

	public Time occ2time(byte[] bs) throws ParseException {
		return new Time(dateTimeFormatter.parse(occ2String(bs)).getTime());
	}

	public Timestamp occ2timestamp(byte[] bs) throws ParseException {
		return new Timestamp(dateTimeFormatter.parse(occ2String(bs)).getTime());
	}
		
	public Timestamp occ2timestamp(byte[] bs, Calendar c) throws ParseException, SQLFeatureNotSupportedException {
		if (bs.length == 23) {
			if (c != null) {
				dateTimeFormatterTZDef.setTimeZone(c.getTimeZone());
			} else {
				dateTimeFormatterTZDef.setTimeZone(Calendar.getInstance().getTimeZone());
			}
			return new Timestamp(dateTimeFormatterTZDef.parse(occ2String(bs)).getTime());
		} else if (bs.length == 30) {
			return new Timestamp(dateTimeFormatterTZ.parse(occ2String(bs)).getTime());
		}
		else {
			throw new ParseException("String length is not of a timestamp: ", bs.length);
		}
	}
		
	public static byte[] int2occ(int x) {
		return Integer.toString(x).getBytes();
	}

	public static byte[] short2occ(short x) {
		return Short.toString(x).getBytes();
	}

	public static byte[] long2occ(long x) {
		return Long.toString(x).getBytes();
	}

	public static int int2occ(int x, byte[] out) {
		return long2occ(x, out);
	}

	public static int short2occ(short x, byte[] out) {
		return long2occ(x, out);
	}
	
	public static int long2occ(long x, byte[] out) {
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

	public static byte[] float2occ(float x) {
		return Float.toString(x).getBytes();
	}

	public static byte[] double2occ(double x) {
		return Double.toString(x).getBytes();
	}

	public static byte[] string2occ(String str) throws UnsupportedEncodingException {
		return str.getBytes("UTF8");
	}

	private byte[] helperDateTime2occ(java.util.Date x) throws UnsupportedEncodingException {
		return string2occ(dateTimeFormatter.format(x));
	}
	
	private byte[] helperDateTime2occ(java.util.Date x, Calendar c) throws UnsupportedEncodingException {
		if (c != null) {
			c.setTime(x);
			dateTimeFormatterTZ.setTimeZone(c.getTimeZone());
		} else {
			dateTimeFormatterTZ.setTimeZone(Calendar.getInstance().getTimeZone());
		}
		return string2occ(dateTimeFormatterTZ.format(x));
	}
	
	@SuppressWarnings("deprecation")
	public byte[] date2occ(Date x) throws UnsupportedEncodingException {
		return helperDateTime2occ(new Date(x.getYear(), x.getMonth(), x.getDate()));
	}

	@SuppressWarnings("deprecation")
	public byte[] date2occ(Date x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2occ(new Date(x.getYear(), x.getMonth(), x.getDate()), c);
	}

	public byte[] time2occ(Time x) throws UnsupportedEncodingException {
		return helperDateTime2occ(x);
	}

	public byte[] time2occ(Time x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2occ(x, c);
	}

	public byte[] timestamp2occ(Timestamp x) throws UnsupportedEncodingException {
		return helperDateTime2occ(x);
	}
	
	public byte[] timestamp2occ(Timestamp x, Calendar c) throws UnsupportedEncodingException {
		return helperDateTime2occ(x, c);
	}	
	
	public static Blob occ2Blob(byte[] bs) {
		return new OccBlob(bs);
	}

	public static Clob occ2Clob(byte[] bs) {
		return new OccClob(bs);
	}

	public static byte[] BigDecimal2occ(BigDecimal x) {
		return x.toPlainString().getBytes();
	}

	public static BigDecimal occ2BigDecimal(byte[] bs) {
		try {
			return new BigDecimal(new String(bs, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
	}

	private static byte hex2int(byte data) throws OccRuntimeException {
		if ((data >= '0') && (data <= '9'))
			return (byte) (data - '0');
		if ((data >= 'A') && (data <= 'F'))
			return (byte) (data - 'A' + 10);
		if ((data >= 'a') && (data <= 'f'))
			return (byte) (data - 'a' + 10);
		throw new OccRuntimeException("Invalid hex digit: " + data);
	}

	public static byte[] hex2Binary(byte[] data) throws OccRuntimeException {
		byte[] ret = new byte[data.length / 2];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = (byte) (hex2int(data[i * 2]) * 16 + hex2int(data[i * 2 + 1]));
		}
		return ret;
	}

	// TODO implement the other conversion functions
}
