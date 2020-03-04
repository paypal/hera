package com.paypal.hera.util;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.paypal.hera.constants.HeraConstants;

public class HeraJdbcUtil {
	static private final String ERRORCODE_PREFIX1 = "ORA-";
	static private final String ERRORCODE_PREFIX2 = "SQL-";
	static private final String DEFAULT_SQLSTATE = "99999";
	static private final Integer ERRORCODE_LEN = 9;             //ORA-xxxxx, SQL-xxxxx
	
	static private final Map<Integer, String> SQLERRORTOSQLSTATEMAP = new HashMap<Integer, String>();
	
	static {
		
		/*Code      Condition                   Oracle Error*/
		
		//00000 	successful completion 		ORA-00000
		SQLERRORTOSQLSTATEMAP.put(0, "00000");
		
		//07008		invalid descriptor count 	SQL-02126
		SQLERRORTOSQLSTATEMAP.put(2126, "07008");
		
		//08003    	connection does not exist 	SQL-02121
		SQLERRORTOSQLSTATEMAP.put(2121, "08003");
		
		//0A000   	feature not supported 		ORA-03000 .. 03099
		for(int i=3000; i<=3099; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "0A000");
		}
		
		//21000 	cardinality violation      ORA-01427
		SQLERRORTOSQLSTATEMAP.put(1427, "21000");
		
		//21000 	cardinality violation      SQL-02112
		SQLERRORTOSQLSTATEMAP.put(2112, "21000");
		
		//22008  	date-time field overflow 	ORA-01800 .. 01899
		for(int i=1800; i<=1899; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "22008");
		}
		
		//22024  	unterminated C string 		ORA-01479 .. 01480
		SQLERRORTOSQLSTATEMAP.put(1479, "22024");
		SQLERRORTOSQLSTATEMAP.put(1480, "22024");
		
		//22025 	invalid escape sequence  	ORA-01424
		SQLERRORTOSQLSTATEMAP.put(1424, "22025");
		
		//23000 	integrity constraint violation 		ORA-00001
		SQLERRORTOSQLSTATEMAP.put(1, "23000");
		
		//23000 	integrity constraint violation 		ORA-02290 .. 02299
		for(int i=2290; i<=2299; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "23000");
		}
		
		//40000 	transaction rollback 		ORA-02091 .. 02092
		SQLERRORTOSQLSTATEMAP.put(2091, "40000");
		SQLERRORTOSQLSTATEMAP.put(2092, "40000");
		
		//60000 	system errors  		ORA-00370 .. 00429
		for(int i=370; i<=429; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "60000");
		}
		
		//60000 	system errors  		ORA-00600 .. 00899
		for(int i=600; i<=899; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "60000");
		}
		
		//60000 	system errors  		ORA-06430 .. 06449
		for(int i=6430; i<=6449; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "60000");
		}
		
		//60000 	system errors  		ORA-07200 .. 07999
		for(int i=7200; i<=7999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "60000");
		}

		//60000		system errors 		ORA-09700 .. 09999
		for(int i=9700; i<=9999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "60000");
		}
		
		//61000		system errors 		ORA-00018 .. 00035
		for(int i=18; i<=35; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "61000");
		}
		
		//61000		system errors 		ORA-00050 .. 00068
		for(int i=50; i<=68; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "61000");
		}
		
		//61000		system errors 		ORA-02376 .. 02399
		for(int i=2376; i<=2399; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "61000");
		}
		
		//61000		system errors 		ORA-04020 .. 04039
		for(int i=4020; i<=4039; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "61000");
		}
		
		//62000 	path name server and detached process errors  	ORA-00100 .. 00120
		for(int i=100; i<=120; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "62000");
		}
		
		//62000 	path name server and detached process errors  	ORA-00440 .. 00569
		for(int i=440; i<=569; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "62000");
		}
		
		//63000 	Oracle*XA and two-task interface errors  ORA-00150 .. 00159
		for(int i=150; i<=159; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "63000");
		}
				
		//63000 	Oracle*XA and two-task interface errors  SQL-02128
		SQLERRORTOSQLSTATEMAP.put(2128, "63000");
		
		//63000 	Oracle*XA and two-task interface errors  ORA-02700 .. 02899
		for(int i=2700; i<=2899; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "63000");
		}

		//63000 	Oracle*XA and two-task interface errors  	ORA-03100 .. 03199
		for(int i=3100; i<=3199; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "63000");
		}
		
		//63000 	Oracle*XA and two-task interface errors  ORA-06200 .. 06249
		for(int i=6200; i<=6249; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "63000");
		}
	
		//64000 	control file, database file, and redo file errors; archival and media recovery errors 	ORA-00200 .. 00369
		for(int i=200; i<=369; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "64000");
		}
		
		//64000   control file, database file, and redo file errors;archival and media recovery errors 	ORA-01100 .. 01250
		for(int i=1100; i<=1250; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "64000");
		}

		//65000  	PL/SQL errors  	ORA-06500 .. 06599
		for(int i=6500; i<=6599; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "65000");
		}
		
		//66000 	SQL*Net driver errors  	ORA-06000 .. 06149
		for(int i=6000; i<=6149; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "66000");
		}
		
		//66000 	SQL*Net driver errors  	ORA-06250 .. 06429
		for(int i=6250; i<=6429; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "66000");
		}
		
		//66000 	SQL*Net driver errors  	ORA-06600 .. 06999
		for(int i=6600; i<=6999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "66000");
		}
		
		//66000  	SQL*Net driver errors   ORA-12100 .. 12299
		for(int i=12100; i<=12299; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "66000");
		}
		
		//66000  	SQL*Net driver errors   ORA-12500 .. 12599
		for(int i=12500; i<=12599; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "66000");
		}
		
		//67000  	licensing errors  	ORA-00430 .. 00439
		for(int i=430; i<=439; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "67000");
		}
		
		//69000 	SQL*Connect errors 	ORA-00570 .. 00599
		for(int i=570; i<=599; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "69000");
		}
		
		//69000 	SQL*Connect errors 	ORA-07000 .. 07199
		for(int i=7000; i<=7199; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "69000");
		}
		
		//90000		debug events	ORA-10000 .. 10999
		for(int i=10000; i<=10999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "90000");
		}
		
		//72000 	SQL execute phase errors 		ORA-01000 .. 01099
		for(int i=1000; i<=1099; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-01400 .. 01489
		for(int i=1400; i<=1489; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
				
		//72000 	SQL execute phase errors 		ORA-01495 .. 01499
		for(int i=1495; i<=1499; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-01500 .. 01699
		for(int i=1500; i<=1699; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-02400 .. 02419
		for(int i=2400; i<=2419; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-02425 .. 02449
		for(int i=2425; i<=2449; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-04060 .. 04069
		for(int i=4060; i<=4069; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-08000 .. 08190
		for(int i=8000; i<=8190; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-12000 .. 12019
		for(int i=12000; i<=12019; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-12300 .. 12499
		for(int i=12300; i<=12499; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//72000 	SQL execute phase errors 		ORA-12700 .. 21999
		for(int i=12700; i<=21999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "72000");
		}
		
		//82100		out of memory (could not allocate)  	SQL-02100
		SQLERRORTOSQLSTATEMAP.put(2100, "82100");
		
		//82101 	inconsistent cursor cache: unit cursor/global cursor mismatch 	SQL-02101
		SQLERRORTOSQLSTATEMAP.put(2101, "82101");
		
		//82102		inconsistent cursor cache: no global cursor entry	SQL-02102
		SQLERRORTOSQLSTATEMAP.put(2102, "82102");
		
		//82103		inconsistent cursor cache: out of range cursor cache reference	SQL-02103
		SQLERRORTOSQLSTATEMAP.put(2103, "82103");
		
		//82104 	inconsistent host cache: no cursor cache available	SQL-02104
		SQLERRORTOSQLSTATEMAP.put(2104, "82104");
		
		//82105		inconsistent cursor cache: global cursor not found	SQL-02105
		SQLERRORTOSQLSTATEMAP.put(2105, "82105");
		
		//82106		inconsistent cursor cache: invalid Oracle cursor number	SQL-02106
		SQLERRORTOSQLSTATEMAP.put(2106, "82106");
		
		//82107		program too old for runtime library		SQL-02107
		SQLERRORTOSQLSTATEMAP.put(2107, "82107");
		
		//82108		invalid descriptor passed to runtime library	SQL-02108
		SQLERRORTOSQLSTATEMAP.put(2108, "82108");
		
		//82109		inconsistent host cache: host reference is out of range		SQL-02109
		SQLERRORTOSQLSTATEMAP.put(2109, "82109");
		
		//82110		inconsistent host cache: invalid host cache entry type	SQL-02110
		SQLERRORTOSQLSTATEMAP.put(2110, "82110");
		
		//82111		heap consistency error	SQL-02111
		SQLERRORTOSQLSTATEMAP.put(2111, "82111");
		
		//82112		unable to open message file		SQL-02113
		SQLERRORTOSQLSTATEMAP.put(2113, "82112");
		
		//82113		code generation internal consistency failed		SQL-02115
		SQLERRORTOSQLSTATEMAP.put(2115, "82113");
		
		//82114		reentrant code generator gave invalid context	SQL-02116
		SQLERRORTOSQLSTATEMAP.put(2116, "82114");
		
		//82115		invalid hstdef argument		SQL-02119
		SQLERRORTOSQLSTATEMAP.put(2119, "82115");
		
		//82116		first and second arguments to sqlrcn both null	SQL-02120
		SQLERRORTOSQLSTATEMAP.put(2120, "82116");
		
		//82117		invalid OPEN or PREPARE for this connection		SQL-02122
		SQLERRORTOSQLSTATEMAP.put(2122, "82117");
		
		//82118		application context not found	SQL-02123
		SQLERRORTOSQLSTATEMAP.put(2123, "82118");
		
		//82119		connect error; can't get error text		SQL-02125
		SQLERRORTOSQLSTATEMAP.put(2125, "82119");
		
		//82120		precompiler/SQLLIB version mismatch.	SQL-02127
		SQLERRORTOSQLSTATEMAP.put(2127, "82120");
		
		//82121		FETCHed number of bytes is odd		SQL-02129
		SQLERRORTOSQLSTATEMAP.put(2129, "82121");
		
		//82122		EXEC TOOLS interface is not available	SQL-02130
		SQLERRORTOSQLSTATEMAP.put(2130, "82122");
		
		//02000 	no data  		            ORA-01095
		SQLERRORTOSQLSTATEMAP.put(1095, "02000");
				
		//02000 	no data  		            ORA-01403
		SQLERRORTOSQLSTATEMAP.put(1403, "02000");
		
		//22001 	string data - right truncation  ORA-01401
		SQLERRORTOSQLSTATEMAP.put(1401, "22001");
				
		//22001 	string data - right truncation  ORA-01406
		SQLERRORTOSQLSTATEMAP.put(1406, "22001");
				
		//22002 	null value - no indicator parameter  ORA-01405
		SQLERRORTOSQLSTATEMAP.put(1405, "22002");
				
		//22002 	null value - no indicator parameter  SQL-02124
		SQLERRORTOSQLSTATEMAP.put(2124, "22002");
			
		//22003 	numeric value out of range  ORA-01426
		SQLERRORTOSQLSTATEMAP.put(1426, "22003");
			
		//22003 	numeric value out of range  ORA-01438
		SQLERRORTOSQLSTATEMAP.put(1438, "22003");
				
		//22003 	numeric value out of range  ORA-01455
		SQLERRORTOSQLSTATEMAP.put(1455, "22003");
				
		//22003 	numeric value out of range  ORA-01457
		SQLERRORTOSQLSTATEMAP.put(1457, "22003");
		
		//22012  	division by zero   	ORA-01476
		SQLERRORTOSQLSTATEMAP.put(1476, "22012");
		
		//22019 	invalid escape character  	ORA-00911
		SQLERRORTOSQLSTATEMAP.put(911, "22019");
		
		//22019 	invalid escape character  	ORA-01425
		SQLERRORTOSQLSTATEMAP.put(1425, "22019");
		
		//22022 	indicator overflow  		ORA-01411
		SQLERRORTOSQLSTATEMAP.put(1411, "22022");
		
		//22023 	invalid parameter value  	ORA-01025
		SQLERRORTOSQLSTATEMAP.put(1025, "22023");
		
		//22023 	invalid parameter value  	ORA-01488
		SQLERRORTOSQLSTATEMAP.put(1488, "22023");
		
		//22023 	invalid parameter value  	ORA-04000 .. 04019
		for(int i=4000; i<=4019; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "22023");
		}
		
		//24000 	invalid cursor state  		ORA-01001 .. 01003
		SQLERRORTOSQLSTATEMAP.put(1001, "24000");
		SQLERRORTOSQLSTATEMAP.put(1002, "24000");
		SQLERRORTOSQLSTATEMAP.put(1003, "24000");
				
		//24000 	invalid cursor state  		ORA-01410
		SQLERRORTOSQLSTATEMAP.put(1410, "24000");
				
		//24000 	invalid cursor state  		ORA-08006
		SQLERRORTOSQLSTATEMAP.put(8006, "24000");
				
		//24000 	invalid cursor state  		SQL-02114
		SQLERRORTOSQLSTATEMAP.put(2114, "24000");
				
		//24000 	invalid cursor state  		SQL-02117
		SQLERRORTOSQLSTATEMAP.put(2117, "24000");
				
		//24000 	invalid cursor state  		SQL-02118
		SQLERRORTOSQLSTATEMAP.put(2118, "24000");
				
		//24000 	invalid cursor state  		SQL-02122
		SQLERRORTOSQLSTATEMAP.put(2122, "24000");
			
		//42000 	syntax error or access rule violation 	ORA-00022
		SQLERRORTOSQLSTATEMAP.put(22, "42000");
		
		//42000 	syntax error or access rule violation 	ORA-00251
		SQLERRORTOSQLSTATEMAP.put(251, "42000");
		
		//42000 	syntax error or access rule violation 	ORA-00900 .. 00999
		for(int i=900; i<=999; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-01031		
		SQLERRORTOSQLSTATEMAP.put(1031, "42000");
		
		//42000 	syntax error or access rule violation 	ORA-01490 .. 01493
		SQLERRORTOSQLSTATEMAP.put(1490, "42000");
		SQLERRORTOSQLSTATEMAP.put(1491, "42000");
		SQLERRORTOSQLSTATEMAP.put(1492, "42000");
		SQLERRORTOSQLSTATEMAP.put(1493, "42000");
		
		//42000 	syntax error or access rule violation 	ORA-01700 .. 01799
		for(int i=1700; i<=1799; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-01900 .. 02099
		for(int i=1900; i<=2099; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-02140 .. 02289
		for(int i=2140; i<=2289; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-02420 .. 02424
		for(int i=2420; i<=2424; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-02450 .. 02499
		for(int i=2450; i<=2499; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-03276 .. 03299
		for(int i=3276; i<=3299; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-04040 .. 04059
		for(int i=4040; i<=4059; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//42000 	syntax error or access rule violation 	ORA-04070 .. 04099
		for(int i=4070; i<=4099; i++) {
			SQLERRORTOSQLSTATEMAP.put(i, "42000");
		}
		
		//44000 	with check option violation 	ORA-01402
		SQLERRORTOSQLSTATEMAP.put(1402, "44000");

	}
	
	
	public static void notSupported(String message) throws SQLException {
		throw new SQLFeatureNotSupportedException(message);
	}
	
	/**
	 * Convert an errorMessage to SQLErrorCode
	 * 
	 * @param errorMessage which needs parsing
	 * @return SQLErrorCode. i.e. ORA-00942 
	 */
	public static String ErrorToSqlErrorCodeConverter (String errorMessage) {
		if (errorMessage == null) {
			return null;
		}
		
		String errorCode = null;
		int fromIndex = errorMessage.indexOf(ERRORCODE_PREFIX1);
		if (fromIndex >=0 && fromIndex + ERRORCODE_LEN <= errorMessage.length()) {
			errorCode = errorMessage.substring(fromIndex, fromIndex + ERRORCODE_LEN);
		} 
		else {
			fromIndex = errorMessage.indexOf(ERRORCODE_PREFIX2);
			if (fromIndex >=0 && fromIndex + ERRORCODE_LEN <= errorMessage.length()) {
					errorCode = errorMessage.substring(fromIndex, fromIndex + ERRORCODE_LEN);
			}
		}
		 
		return errorCode;
	}
	
	/**
	 * Convert an errorMessage to SQLState
	 * 
	 * @param errorMessage which needs conversion
	 * @return SQLState and vendor code
	 */
	public static Pair<String, Integer> ErrorToSqlStateAndVendorCodeConverter (String errorMessage) {
		String errorCode = ErrorToSqlErrorCodeConverter(errorMessage);
		Pair<String, Integer> errInfo = new Pair<String, Integer>(DEFAULT_SQLSTATE, 0);
		if (errorCode != null && errorCode.length() > 4) {
			Integer code = Integer.valueOf(errorCode.substring(4));
			String strCode = (String)SQLERRORTOSQLSTATEMAP.get(code);
			if (strCode != null)
				errInfo.setFirst(strCode);
			errInfo.setSecond(code);			
		}
		return errInfo;
		
	}

	/**
	The added information between “error code” and “error info” is the following, “num_data (=2X#errors)”  “dml offset” “dml code”.
	
	For example, if to insert 5 rows (id 1-5), row 4 is already there before insert. We got,
	“2 3 1” Where 
	•	2 means 2 numbers to read
	•	3 means the 4th row (offset start from 0)
	•	1 means ORA error 1, which is unique violation
	DB side would have row 1-3 and row 5 inserted. Including row 4 before insertion, DB side has row 1-5 after batch insert.
	
	This additional information is added only when #dml&lt;1 and #dml errors &lt; 0.

	@param errorMessage To be parsed
	@param _num_queries Size of array to allocate and return
	@return Array of counts or error codes
	 */
	public static int[] getArrayCounts(String errorMessage, int _num_queries) {
		int[] counts = new int[_num_queries];
		for (int i = 0; i < _num_queries; i++) {
			counts[i] = Statement.SUCCESS_NO_INFO;
		}
		int fromIndex = errorMessage.indexOf(' ') + 1;
		int index = errorMessage.indexOf(' ', fromIndex);
		int count = 0;
		try {
			count = Integer.parseInt(errorMessage.substring(fromIndex, index)) / 2;
		} catch (Exception ex) {
			for (int i = 0; i < _num_queries; i++) {
				counts[i] = Statement.EXECUTE_FAILED;
			}
			return counts;
		}
		index++;
		for (int i = 0; i < count; i++) {
			int endIndex = errorMessage.indexOf(' ', index);
			counts[Integer.parseInt(errorMessage.substring(index, endIndex))] = Statement.EXECUTE_FAILED;
			index = errorMessage.indexOf(' ', endIndex + 1) + 1;
		}
		return counts;
	}

	public static int getScuttleID(byte[] _data) {

		String strValue;
		try {
			strValue = new String(_data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new NumberFormatException();
		}
		long l = Long.parseLong(strValue);
		byte[] lByte = getByte( l);
		int hash = MurmurHash3.murmurhash3_x86_32(lByte, 0, lByte.length, 0x183d1db4);
		return (int) ((hash & 0xffffffffl) % HeraConstants.MAX_SCUTTLE_BUCKETS);
	}


	public static byte[] getByte(long l) {
		final byte[] buffer = new byte[8];
		for(int i = 0; i<8;i++)
		{
			final long ll = 0xff & (l >> (i * 8));
			buffer[i] = (byte) ll;
		}
		return buffer;
	}

}
