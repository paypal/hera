package dummy

/*
* utils contains all the MySQL protocol basic types (integer, string)
* manipulation functions, connection/command phase packet types, command bytes,
* and capability flag bit masks for the mock server.
*/

import (
     "log"
     "fmt"
     "bytes"
     "strings"
)

/*== CONSTANTS ===============================================================*/


/* Data sizes. Integers can be stored in 1, 2, 3, 4, 6, or 8 bytes.
* The maximum packet size that can be sent between client and server
* is (1 << 24) - 1 bytes. The header size (of a packet) is always 4 bytes.
* See WritePacket() in connection.go for what a packet looks like.
*     https://dev.mysql.com/doc/internals/en/integer.html
*/
const (
     MAX_PACKET_SIZE     int = (1 << 24) - 1
     HEADER_SIZE         int = 4
     INT1                int = 1
     INT2                int = 2
     INT3                int = 3
     INT4                int = 4
     INT6                int = 6
     INT8                int = 8
)

/* String types. Strings are sequences of bytes and can be categorized into
* the following types below.
*     https://dev.mysql.com/doc/internals/en/string.html
* Note that VARSTR is currently NOT SUPPORTED.
*/
type string_t uint
const (
	EOFSTR string_t = iota   // rest of packet string
	NULLSTR                  // null terminated string
     FIXEDSTR                 // fixed length string with known hardcoded length
     VARSTR                   // variable length string
     LENENCSTR                // length encoded string prefixed with lenenc int
)

/* The different types of packets that can be sent over a connection
* according to MySQL protocol from the server's side. Handshakev9 is
* apparently obsolete, so it is not included.
*
*  https://dev.mysql.com/doc/internals/en/mysql-packet.html
*  https://dev.mysql.com/doc/internals/en/generic-response-packets.html
*/
type packet_t uint
const (
     OK                       packet_t = 1 << iota
     ERR
     EOF
     HANDSHAKEv10
     RESULTSET
)

/* Capability flag bit mask. Bitwise | the desired capabilities together
* when configuring the server, like this, to give the server those capabilities:
*
*   capabilities := CLIENT_LONG_PASSWORD | CLIENT_SSL | CLIENT_PROTOCOL_41
*
* Since this is a dummy server, none of the capabilities will actually 'mean'
* or 'do' anything. You should just set the minimum flags so that
* the client is compatible.
*
* https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
*/
type cflag uint
const (
     CLIENT_LONG_PASSWORD                    cflag = 1 << (iota)
     CLIENT_FOUND_ROWS
     CLIENT_LONG_FLAG
     CLIENT_CONNECT_WITH_DB
     CLIENT_NO_SCHEMA
     CLIENT_COMPRESS
     CLIENT_ODBC
     CLIENT_LOCAL_FILES
     CLIENT_IGNORE_SPACE
     CLIENT_PROTOCOL_41
     CLIENT_INTERACTIVE
     CLIENT_SSL
     CLIENT_IGNORE_SIGPIPE
     CLIENT_TRANSACTIONS
     CLIENT_RESERVED
     CLIENT_RESERVED2
     CLIENT_MULTI_STATEMENTS
     CLIENT_MULTI_RESULTS
     CLIENT_PS_MULTI_RESULTS
     CLIENT_PLUGIN_AUTH
     CLIENT_CONNECT_ATTRS
     CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
     CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS
     CLIENT_SESSION_TRACK
     CLIENT_DEPRECATE_EOF
     CLIENT_SSL_VERIFY_SERVER_CERT 	    cflag = 1 << 30
     CLIENT_OPTIONAL_RESULTSET_METADATA     cflag = 1 << 25
     CLIENT_REMEMBER_OPTIONS	              cflag = 1 << 31
)

/* Command byte which is the first byte in a command packet. Signifies
* what command the client wants the server to carry out. The command bytes
* are consistent with MySQL 4.1.
*    https://dev.mysql.com/doc/internals/en/command-phase.html
*/
type COM byte
const (
     COM_SLEEP                               COM = iota // 0
     COM_QUIT
     COM_INIT_DB
     COM_QUERY
     COM_FIELD_LIST
     COM_CREATE_DB
     COM_DROP_DB
     COM_REFRESH
     COM_SHUTDOWN
     COM_STATISTICS
     COM_PROCESS_INFO // 10
     COM_CONNECT
     COM_PROCESS_KILL
     COM_DEBUG
     COM_PING
     COM_TIME
     COM_DELAYED_INSERT
     COM_CHANGE_USER
     COM_BINLOG_DUMP
     COM_TABLE_DUMP
     COM_CONNECT_OUT // 20
     COM_REGISTER_SLAVE
     COM_STMT_PREPARE
     COM_STMT_EXECUTE
     COM_STMT_SEND_LONG_DATA
     COM_STMT_CLOSE
     COM_STMT_RESET
     COM_SET_OPTION
     COM_STMT_FETCH
     COM_RESET_CONNECTION
     COM_DAEMON // 30
)

/* Each query needs to be parsed so that it's recorded the number of parameters,
* number of columns, and stmtid associated with the query. This is important
* because the number of column definition packets that are sent out
* are dependent on numParams and numColumns. The stmtid and numParams and/or
* numColumns must be consistent between server and client, so it's important
* to keep track. The colNames are basically the schema of the projection
* specified in the query.
*/
type QueryInfo struct {
     numParams int
     colNames []string
     stmtid int
     query string
     numColumns int
}

/*== FUNCTIONS ===============================================================*/

/*-- ARITHMETIC --------------------------------------------------------------*/

/* min returns the minimum of two functions. inline */
func min(a int, b int) (int) {
     if a < b { return a }
     return b
}

/* Checks bitmask capability flag against server/client/connection configuration
* and returns true if the bit is set, otherwise false.
*/
func isFlagSet(cflags uint32, c cflag) (bool) {
     if (cflags & uint32(c)) != 0 {
          return true
     }
     return false
}

/* Checks that size of slice is enough for the incoming data. */
func checkSize(sz1 int, sz2 int) {
	if sz1 < sz2 {
		log.Fatal(fmt.Sprintf("Array size %d, expected %d", sz1, sz2))
	}
}


/*-- WRITING DATA ------------------------------------------------------------*/

/* Writes an unsigned integer n as a fixed length integer int<l> into the slice
* data. The intptr pos keeps track of where in the buffer (data) we are
* before and after writing to the buffer.
*/
func WriteFixedLenInt(data []byte, l int, n int, pos *int) {
     // Check that the length of data is enough to accomodate the length
     // of the encoding.
	checkSize(len(data[*pos:]), l)
	switch l {
		case 8:
			data[*pos + 7] = byte(n >> 56)
			data[*pos + 6] = byte(n >> 48)
			fallthrough
		case 6:
			data[*pos + 5] = byte(n >> 40)
			data[*pos + 4] = byte(n >> 32)
			fallthrough
		case 4:
			data[*pos + 3] = byte(n >> 24)
			fallthrough
		case 3:
			data[*pos + 2] = byte(n >> 16)
			fallthrough
		case 2:
			data[*pos+1] = byte(n >> 8)
			fallthrough
		case 1:
			data[*pos] = byte(n)
		default:
			log.Fatal(fmt.Sprintf("Unexpected size %d", l))
	}
     // Move the index tracker.
	*pos += l
}


/* Writes an unsigned integer n as a length encoded integer
* into the slice data. Checks that the data byte array is big enough.
* The intptr pos keeps track of where in the buffer (data) we are
* before and after writing to the buffer.
*/
func WriteLenEncInt(data []byte, n uint64, pos *int) {
     // Determine the length encoded integer.
	l := 1
	if n >= 251 && n < (1 << 16) {
		l = 3
	} else if n >= (1 << 16) && n < (1 << 24) {
		l = 4
	} else if n >= (1 << 24) {
		l = 9
	}

     // Check that the data byte array is big enough for the desired length.
	checkSize(len(data[*pos:]), l)

     // Write the length encoded integer into the data array.
	if l == 1 {
		WriteFixedLenInt(data, l, int(n), pos)
	} else {
		switch l {
			case 3:
				data[*pos] = byte(0xfc)
			case 4:
				data[*pos] = byte(0xfd)
			case 9:
				data[*pos] = byte(0xfe)
		}
		*pos++
		WriteFixedLenInt(data, l-1, int(n), pos)
	}

}

/* Writes a string str into the slice data. The method of writing is different
* depending on the string type. l is supposed to be an optional argument
* for when the length needs to specified (i.e. FIXEDSTR, EOFSTR). The intptr
* pos keeps track of where in the buffer (data) we are before and after writing
* to the buffer.
*/
func WriteString(data []byte, str string, stype string_t, pos *int, l int) {
     switch stype {
          case NULLSTR:
               checkSize(len(data[*pos:]), len(str))
               // Write the string and then terminate with 0x00 byte.
               copy(data[*pos:], str)
               checkSize(len(data[*pos:]), len(str) + 1)
               *pos += len(str)
               data[*pos] = 0x00
               *pos++

          case LENENCSTR:
               // Write the encoded length.
               WriteLenEncInt(data, uint64(len(str)), pos)
               // Then write the string as a FIXEDSTR.
               WriteString(data, str, FIXEDSTR, pos, l)

          case FIXEDSTR:

               checkSize(len(data[*pos:]), l)
               // Pads the string with 0's to fill the specified length l.
               copy(data[*pos:*pos+l], str)
               *pos += l

          case EOFSTR:

               checkSize(len(data[*pos:]), len(str))
               // Copies the string into the data.
               *pos += copy(data[*pos:], str)
     }
}


/*-- READING DATA ------------------------------------------------------------*/

/* Reads an unsigned integer n as a fixed length integer
* int<l> from the slice data. The intptr pos keeps track of where in the buffer
* (data) we are before and after writing to the buffer.
* Basically what happens is that this bit-shifts the elements accordingly
* and bit-wise ORs all of them together to get the original integer back.
*/
func ReadFixedLenInt(data []byte, l int, pos *int) int {
	checkSize(len(data[*pos:]), l)
     n := uint(0)
	switch l {
          case INT8:
			n |= uint(data[*pos + 7]) << 56
			n |= uint(data[*pos + 6]) << 48
			fallthrough
		case INT6:
			n |= uint(data[*pos + 5]) << 40
			n |= uint(data[*pos + 4]) << 32
			fallthrough
		case INT4:
			n |= uint(data[*pos + 3]) << 24
			fallthrough
		case INT3:
			n |= uint(data[*pos + 2]) << 16
			fallthrough
		case INT2:
			n |= uint(data[*pos+1]) << 8
			fallthrough
		case INT1:
			n |= uint(data[*pos])
		default:
			log.Fatal(fmt.Sprintf("Unexpected size %d", l))
	}
	*pos += l

     return int(n)
}


/* Reads an unsigned integer n as a length encoded integer
* from the slice data. */
func ReadLenEncInt(data []byte, pos *int) int {
     l := 0         // length of the length encoded integer

     // Check the first byte to determine the length.
     fb := byte(data[*pos])

     // If the first byte is < 0xfb, then l = 1.
     if fb < 0xfb {
          l = 1
     }

	if l == 1 {
          // Read 1 byte for lenenc<1>.
		return ReadFixedLenInt(data, INT1, pos)
	}

	*pos++

     // Otherwise read the appropriate length according to the
     // encoded length.
	switch fb {
		case 0xfc: // 2-byte integer
               return ReadFixedLenInt(data, INT2, pos)
		case 0xfd: // 3-byte integer
			return ReadFixedLenInt(data, INT3, pos)
          default : // 8-byte integer
               return ReadFixedLenInt(data, INT8, pos)
	}
}


/* Reads a string str from the slice data. The method of reading is different
* depending on the string type. l is supposed to be an optional argument
* for when the length needs to specified (i.e. FIXEDSTR (specified), and
* EOFSTR, where the length of the string to be read in is calculated from
* current position and remaining length of packet).
*/
func ReadString(data []byte, stype string_t, pos *int, l int) string {
     buf := bytes.NewBuffer(data[*pos:])
     switch stype {
          case NULLSTR:
               line, err := buf.ReadBytes(byte(0x00))
     		if err != nil {
     		    log.Fatal(err)
     		}
               *pos += len(line)
               return string(line)
          case LENENCSTR:
               n := ReadLenEncInt(data, pos)
               if n == 0 {
                    break
               }
               buf.ReadByte()
               temp := make([]byte, n)
               n2, err := buf.Read(temp)
               if err != nil {
                    log.Fatal(err)
               } else if n2 != n {
                    log.Fatal(fmt.Sprintf("Read %d, expected %d", n2, n))
               }
               *pos += n
               return string(temp)
          case FIXEDSTR, EOFSTR:
               temp := make([]byte, l)
               n2, err := buf.Read(temp)
               if err != nil {
                    log.Fatal(err)
               } else if n2 != l {
                    log.Fatal(fmt.Sprintf("Read %d, expected %d", n2, l))
               }
               *pos += l
               return string(temp)
     }
     return ""
}


/* Parse SQL query to return an object stored with stmt_id, number of parameters,
* number of columns, the query, and a list of the column names. */
func ParseQuery(query string, stmtid int) *QueryInfo {
     // Remove any comments that start before the command.
     if strings.HasPrefix(query, "/*cmd*/") {
          query = query[len("/*cmd*/"):]
     }

     // Get the first word in the query, which is the command.
     words := strings.Fields(query)
     var columns []string
     var numParams int
     var numColumns int

     // Handle the command accordingly
     switch strings.ToLower(words[0]) {
          case "select":
               // WILL eventually fix to use a regex parser.
               split1 := strings.Split(query, "from")
               // If there was no from, then set the numParams to...0?
               // UNCLEAR
               if split1[0] == query {
                    // this should be accurate
                    numParams = 0
                    columns = strings.Split(split1[0], ",")
                    numColumns = len(columns)
               } else {
                    // Split the columns (listed after SELECT but before FROM)
                    //      // by ',''
                    //      columns = strings.Split(split1[0], ",")
                    //      numParams = len(columns)


                    // set temporarily to the com_stmt_prepare
                    // expected in coordinator_basic
                    numParams = 1
                    numColumns = 2
               }

          case "update":
               // Isolate the string after keyword 'SET'
               split1 := strings.Split(query, "set")
               // Isolate the string before keyword 'WHERE'
               split2 := strings.Split(split1[1], "WHERE")
               // Split the string of column names separated by ','
               // which are between keywords SET and WHERE
               columns = strings.Split(split2[0], ",")
               numColumns = len(columns)

          case "insert":
               // Isolate the string after '('
               split1 := strings.Split(query, "(")
               // Isolate the string before ')'
               split2 := strings.Split(split1[1], ")")
               // Split the string of column names separated by ','
               // which are between table name and keyword VALUES
               columns = strings.Split(split2[0], ",")
               numParams = len(columns)

     }

     // Return QueryInfo struct.
     return &QueryInfo{numParams:numParams, colNames:columns,
                    stmtid:stmtid, query:query, numColumns:numColumns}
}
