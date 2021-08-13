// Copyright 2019 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package mysqlpackets contains encoding and decoding functions in MySQL protocol
// format
package postgrespackets

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/paypal/hera/utility/encoding"
	"github.com/paypal/hera/utility/logger"
)

/* ==== CONSTANTS ============================================================*/
/* ---- STRING TYPES. ----------------------------------------------------------
* Strings are sequences of bytes and can be categorized into the following
* types below.
*     https://dev.mysql.com/doc/internals/en/string.html
* Note that VARSTR is currently NOT SUPPORTED.
 */
type string_t uint

const (
	EOFSTR    string_t = iota // rest of packet string
	NULLSTR                   // null terminated string
	FIXEDSTR                  // fixed length string with known hardcoded length
	VARSTR                    // variable length string -- as of right now, unused
	LENENCSTR                 // length encoded string prefixed with lenenc int
)

// ---- Data sizes. ------------------------------------------------------------
const (
	MAX_PACKET_SIZE int = (1 << 24) - 1
	HEADER_SIZE     int = 4
	INT1            int = 1
	INT2            int = 2
	INT3            int = 3
	INT4            int = 4
	INT6            int = 6
	INT8            int = 8
)

// https://www.postgresql.org/docs/current/datatype-numeric.html
var EnumFieldTypes = map[string]int{
	"DECIMAL":  0x00, // PSQL - user-specified precision, exact
	"SMALLINT": 0x02, // PSQL - small range integer
	"INTEGER":  0x04, // PSQL - typical choice of integer
	"BIGINT":   0x06, // PSQL - large range integer

}

type Packager struct {
	reader io.Reader
	writer io.Writer
	// sqid   int // Keeps track
	commandID int
}

/* ==== FUNCTIONS ============================================================*/

/* ---- HERA USE -------------------------------------------------------------*/
// Creates a Packet from the reader, reading exactly as many
// bytes as necessary. Assumes that the encoding.Packet being read is a COMMAND PACKET
// only. Used for incoming requests from client.
// func NewInitPSQLPacket(_reader io.Reader) (*encoding.Packet, error) {
func NewInitPSQLPacket(conn net.Conn) (*encoding.Packet, error) {
	logger.GetLogger().Log(logger.Info, "Inside NewInitPSQLPacket")
	ns := &encoding.Packet{}

	// https://www.postgresql.org/docs/current/protocol-message-formats.html

	// Read in the header
	_reader := bufio.NewReader(conn)
	tmp := make([]byte, 92)
	_, err := io.ReadFull(_reader, tmp)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "packet reading err", err)
	}

	// _, err = _reader.Read(tmp)

	// if err != nil {
	// 	logger.GetLogger().Log(logger.Info, "reader err", err)
	// }

	logger.GetLogger().Log(logger.Info, "what is tmp", tmp)

	idx := 0
	commandID := ReadString(tmp, FIXEDSTR, &idx, 1)
	payloadLength := ReadFixedLenInt(tmp, INT4, &idx)

	logger.GetLogger().Log(logger.Info, "what is commandID", commandID)
	logger.GetLogger().Log(logger.Info, "what is payloadLength", payloadLength)

	if payloadLength == 0 {
		return nil, nil
	}
	// The total length is the header + payload, given by buff.Len() + payload
	// length read from the packet

	totalLen := payloadLength + HEADER_SIZE

	ns.Length = payloadLength
	ns.CommandID = commandID
	ns.Serialized = make([]byte, totalLen+1)
	bytesRead := 1
	// Copy the header over into ns.Serialized
	copy(ns.Serialized[bytesRead:], tmp)
	// Mark number of bytes already read
	bytesRead += len(tmp)

	// Read in the payload
	var n int
	for bytesRead < totalLen+1 {
		n, err = _reader.Read(ns.Serialized[bytesRead:])
		if err != nil {
			return nil, err
		}
		bytesRead += n
	}
	if bytesRead-1 != totalLen {
		return nil, errors.New(fmt.Sprintf("expected %d bytes, instead got %d,", totalLen, bytesRead-1))
	}

	// Read command byte, which is the first byte after the header
	ns.Cmd = int(ns.Serialized[HEADER_SIZE+1])
	ns.Payload = ns.Serialized[HEADER_SIZE+1:]
	// ns.Payload = ns.Serialized[next:totalLen]
	// ns.IsMySQL = false
	ns.IsPostgreSQL = true

	return ns, nil
}

// Creates a Packet from the reader, reading exactly as many
// bytes as necessary. Assumes that the encoding.Packet being read is a COMMAND PACKET
// only. Used for internal Hera communication only.
func NewPSQLPacket(_reader io.Reader) (*encoding.Packet, error) {
	logger.GetLogger().Log(logger.Info, "Inside NewPostgreSQLPacket")
	ns := &encoding.Packet{}

	var ptype = make([]byte, INT1)
	var tmp = make([]byte, INT4)
	var err error

	logger.GetLogger().Log(logger.Info, "log 1")

	// Read in the indicator byte
	_, err = _reader.Read(ptype)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "found error")
	}

	logger.GetLogger().Log(logger.Info, "log 2")

	// // Check packet indicator byte.
	if len(ptype) != 0 && ptype[0] != 0 {
		if int(ptype[0]) == 1 {
			return nil, encoding.WRONGPACKET
		}
		return nil, encoding.UNKNOWNPACKET
	}
	logger.GetLogger().Log(logger.Info, "log 3")

	// Read the header into tmp
	_, err = _reader.Read(tmp)
	logger.GetLogger().Log(logger.Info, "Read it in")

	idx := 0
	// commandID := ReadFixedLenInt(tmp, INT1, &idx)
	commandID := ReadString(tmp, FIXEDSTR, &idx, 1)
	payloadLength := ReadFixedLenInt(tmp, INT4, &idx)

	if payloadLength == 0 {
		return nil, nil
	}
	// The total length is the header + payload, given by buff.Len() + payload
	// length read from the packet

	totalLen := payloadLength + HEADER_SIZE // NOT SURE IF THIS IS RIGHT FOR PSQL

	ns.Length = payloadLength
	ns.CommandID = commandID
	// ns.Serialized = make([]byte, totalLen+1)
	ns.Serialized = make([]byte, totalLen+1) // + 1 is for the indicator byte
	ns.Serialized[0] = ptype[0]

	bytesRead := 1
	// Copy the header over into ns.Serialized
	copy(ns.Serialized[bytesRead:], tmp)
	// Mark number of bytes already read
	bytesRead += len(tmp)

	// Read in the payload
	var n int
	for bytesRead < totalLen+1 {
		n, err = _reader.Read(ns.Serialized[bytesRead:])
		if err != nil {
			return nil, err
		}
		bytesRead += n
	}
	if (bytesRead - 1) != totalLen {
		return nil, errors.New(fmt.Sprintf("expected %d bytes, instead got %d,", totalLen, bytesRead-1))
	}

	// Read command byte, which is the first byte after the header
	ns.Cmd = int(ns.Serialized[HEADER_SIZE+1])
	// Set the payload of the packet.
	ns.Payload = ns.Serialized[HEADER_SIZE+1:]
	ns.IsPostgreSQL = true
	logger.GetLogger().Log(logger.Info, "Ready to return")

	return ns, nil
}

// // NewPacketFrom creates a packet from command and payload.
// // Although, I don't know when this would ever be used by the server, but maybe
// // it will be of use from the client!
// /* NOTE: READ THIS
// * There's some sorcery behind the scenes here. In the netstring implementation
// * of the Packaging interface, the argument passed in for _cmd is genuinely
// * used as a command int / opcode. However, since MySQL packets contain
// * the command byte already in the payload and NewPacketFrom is likely to be
// * used for /sending/ packets instead, we've jury-rigged this so that it
// * follows the interface but allows us to keep track of the sequence_id (which
// * notably netstring doesn't have). Then we can return other kinds of response
// * packets! So note that NewPacketFrom is used by the server to construct
// * packets to send to the client.
//  */
func NewPSQLPacketFrom(sqid int, _payload []byte) *encoding.Packet {

	// Grab the payload length
	payloadLen := len(_payload) - 1

	// Create an empty encoding.Packet
	ns := &encoding.Packet{}

	if payloadLen == 0 {
		return ns
	}

	// Read the command byte from the payload! ;)
	// ns.Cmd = int(_payload[0])

	// Create the full packet which has the header and the payload.
	ns.Serialized = make([]byte, 1+payloadLen)

	ns.Length = payloadLen

	ns.CommandID = make([]byte, 1)
	ns.CommandID[0] = _payload[0]

	ns.Payload = _payload[1:]
	ns.IsPostgreSQL = true

	// Write in header
	idx := 0

	WriteString(ns.Serialized, string(_payload[0]), FIXEDSTR, &idx, 1)
	// WriteFixedLenInt(ns.Serialized, INT4, payloadLen, &idx)
	// WriteString(ns.Serialized, string(_payload[5:]), FIXEDSTR, &idx, payloadLen-5)

	copy(ns.Serialized[idx:], _payload)

	return ns
}

// Write multiple (or one) packets. Copied this over from mocksqlsrv WritePacket code.
func (p *Packager) WritePacket(_payload []byte) ([]*encoding.Packet, error) {

	/* Set current payload length. */
	length := len(_payload) // Keeps track of the remaining length to be written in _payload
	pidx := 0               // Keeps track of reading position in _payload

	numPackets := 0

	var packets []*encoding.Packet

	for length > 0 {
		/* Determine packetLength, capped by MAX_PACKET_SIZE. */
		packetsize := min(length, MAX_PACKET_SIZE)
		numPackets++

		packets = append(packets, NewPSQLPacketFrom(p.commandID, _payload[pidx:pidx+packetsize]))

		pidx += packetsize
		if pidx > len(_payload) {
			return packets, errors.New("index range exceeds payload; length out of bonds")
		}

		length -= packetsize
		p.commandID++
	}

	return packets, nil
}

// // NewPacketReader creates a Reader, that maintains the state / aka sequence_id
// // for packets sent to the server
func NewPackager(_reader io.Reader, _writer io.Writer) *Packager {
	return &Packager{reader: _reader, writer: _writer}
}

// // ReadNext returns the next packet from the stream.
// // Note: in case of multiple packets bigger than 16 MB the Reader will buffer
// // some packets, a different function will probably have to be used. This is
// // just for grabbing one packet from the stream. encoding.Packets are not embedded.
func (p *Packager) ReadNext() (ns *encoding.Packet, err error) {
	// Read in a packet from the packager's reader.
	logger.GetLogger().Log(logger.Info, "Inside readnext")
	pkt, err := NewPSQLPacket(p.reader)
	if err != nil {
		logger.GetLogger().Log(logger.Info, "newsqlpacket", err)
		return nil, err
	}
	logger.GetLogger().Log(logger.Info, "Back Inside readnext")
	// Set the sequence id to what is already in the packet
	// p.sqid = pkt.Sqid
	// p.commandID = pkt.CommandID
	return pkt, err
}

// Length of length encoded string is length of the lenenc and length of the string
func calculateLenEncStr(s string) int {
	return calculateLenEnc(uint64(len(s))) + len(s)
}

// Result sets function
// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_com_query_response_text_resultset_column_definition.html
// This is specifically for reconstructing ColumnDefinition41 packets.
func (p *Packager) ColumnDefinition(colName string, colType *sql.ColumnType) []byte {
	// TODO: Reconstruct column definition packet... Unsure how this will be done because what is returned from
	//  a sql.Prepare(...) is a sql.Stmt. The sql.Rows is where we get sql.ColumnTypes from, which happens AFTER
	//  we execute the query. But sql.Rows also does not expose all of the necessary fields to reconstruct the
	//  original ColumnDefinition packet.

	// Somehow, we will gather information from the sql.ColumnType or put in filler garbage information for now.
	ctl := "def"
	schema := "temp-schema"
	table := "temp-table"
	org_table := "temp-table"
	name := colName
	org_name := colType.Name()
	totalLen := calculateLenEncStr("def") + calculateLenEncStr(schema) + calculateLenEncStr(table) + calculateLenEncStr(org_table) +
		calculateLenEncStr(org_name) + calculateLenEnc(uint64(0x0c)) + INT2 + INT4 + INT1 + INT2 + INT1
	payload := make([]byte, totalLen)
	pos := 0
	colLength, ok := colType.Length()
	if !ok {
		logger.GetLogger().Log(logger.Debug, "colType.Length()", colLength)
	}

	cTypeInt := EnumFieldTypes[colType.DatabaseTypeName()] // returns sql column type as an int

	// The flags encode a lot of information about what the column is. If it can have NULL values, is it unique,
	// is it a primary key, is it autoincrement, is it group, etc. This is the information that gets lost between
	// using the go-sql-driver and communication with the MySQL database.

	// This section is to determine whether or not the column is of a nullable type or not.
	var flags int
	nable, ok := colType.Nullable()
	if !ok {
		if nable {
			flags = 0
		} else {
			flags = 1
		}
	} else {
		flags = 1
	}

	// This section determines the precision (number of decimal digits to show) for the column.
	var prec int
	switch cTypeInt {
	case 0x01 /* tiny int */, 0x02 /* short */, 0x03 /* long */, 0x08 /* longlong */, 0x09 /* int24 */, 0xfe /* char */ :
		prec = 0x00
	case 0xfd /* var_string */, 0x0f /* varchar */, 0x05 /* double */, 0x04 /* float */ :
		prec = 0x1f
	case 0x00 /* decimal */, 0xf6 /* new_decimal*/ :
		tmp, _, ok := colType.DecimalSize()
		if !ok {
			logger.GetLogger().Log(logger.Warning, "Decimal size")
		}
		prec = int(tmp)
	}

	// Write catalog
	WriteString(payload, ctl, LENENCSTR, &pos, 0)
	// Write schema
	WriteString(payload, schema, LENENCSTR, &pos, 0)
	// Write table
	WriteString(payload, table, LENENCSTR, &pos, 0)
	// Write org_table
	WriteString(payload, org_table, LENENCSTR, &pos, 0)
	// Write name
	WriteString(payload, name, LENENCSTR, &pos, 0)
	// Write org_name
	WriteString(payload, org_name, LENENCSTR, &pos, 0)
	// write length of fixed length fields
	WriteLenEncInt(payload, 0x0c, &pos)
	// char set (temporarily utf8_general_ci which is 0x21)
	WriteFixedLenInt(payload, INT2, 0x21, &pos)
	// column-length
	WriteFixedLenInt(payload, INT4, int(colLength), &pos)
	// column scan type
	WriteFixedLenInt(payload, INT1, cTypeInt, &pos)
	// flags (mainly used for checking nullable)
	WriteFixedLenInt(payload, INT2, flags, &pos)
	// decimals
	WriteFixedLenInt(payload, INT1, prec, &pos)
	// filler
	WriteFixedLenInt(payload, INT2, 0x00, &pos)

	/*
	* There should be a case for [if command was COM_FIELD_LIST], but that's unlikely to be supported
	* at this time.
	 */

	return payload
}

// Stmt Prepare OK content pre-Column definition (if any)
// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
// This is specifically for ColumnDefinition41 packets.
func StmtPrepareOK(stmt_id, num_columns, num_params int) []byte {
	payload := make([]byte, INT1 /* status */ +INT4 /* stmtid */ +INT2 /* cols */ +INT2 /* params */ +INT1 /* filler */ +INT2 /* warnings */)
	pos := 0
	// Write status
	WriteFixedLenInt(payload, INT1, 0x00, &pos)
	// Write stmt_id
	WriteFixedLenInt(payload, INT4, stmt_id+1, &pos)
	// Write num_columns
	WriteFixedLenInt(payload, INT2, num_columns, &pos)
	// Write num_params
	WriteFixedLenInt(payload, INT2, num_params, &pos)

	logger.GetLogger().Log(logger.Info, "Writing OK packet payload:", payload)
	return payload
}

//
//// Result sets function .... sigh
func (p *Packager) ResultsetRow(rows *sql.Rows) []byte {
	cols, err := rows.Columns()
	if err != nil {
		logger.GetLogger().Log(logger.Warning, err.Error())
	}
	// null_bitmap_length := (len(cols) + 7 + 2) / 8
	readCols := make([]interface{}, len(cols))
	writeCols := make([]sql.NullString, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	for rows.Next() {
		err = rows.Scan(readCols...)
	}
	for i := range writeCols {
		if writeCols[i].Valid {
		}
	}
	return []byte{}
}

// Result sets function for the single packet containing the length encoded integer. Returns payload and updated
// stmtid
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (p *Packager) Resultset(column_count, stmtid int, rows *sql.Rows) []byte {
	cpLen := calculateLenEnc(uint64(column_count))
	count_packet := make([]byte, cpLen)
	pos := 0
	WriteLenEncInt(count_packet, uint64(column_count), &pos)
	return count_packet
}

/*---- COMMON PACKETS ----------------------------------------------------------
* Packets that are frequently used, like ERR packet or OK packet or EOF packet
* are written below.
 */

// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
// func OKPacket(affectedRows int, lastInsertId int, capabilities uint32, msg string) []byte {
// 	pLen := 1 + calculateLenEnc(uint64(affectedRows)) + calculateLenEnc(uint64(lastInsertId))
// 	if Supports(capabilities, CLIENT_PROTOCOL_41) {
// 		pLen += 4
// 	}
// 	payload := make([]byte, pLen)
// 	pos := 0
// 	// Write OK packet header
// 	WriteFixedLenInt(payload, INT1, 0x00, &pos)

// 	// Write affected_rows
// 	WriteLenEncInt(payload, uint64(affectedRows), &pos)
// 	// Write last_insert_id
// 	WriteLenEncInt(payload, uint64(lastInsertId), &pos)

// 	if Supports(capabilities, CLIENT_PROTOCOL_41) {
// 		WriteFixedLenInt(payload, INT2 /* status_flags */, 0x00, &pos)
// 		WriteFixedLenInt(payload, INT2 /* warnings */, 0x00, &pos)
// 	}

// 	/* There's several things to do with client capabilities....that are all ignored
// 	*
// 	*  if capabilities & CLIENT_PROTOCOL_41 { write status_flags int<2> and warnings int<2>}
// 	*  elseif capabilities & CLIENT_TRANSACTIONS { status_flags <2> }
// 	*  if capabilities & CLIENT_SESSION_TRACK { info string<lenenc> ;
// 	*     if status_flags & SERVER_SESSION_STATE_CHANGED { session_state_changes string<lenenc> }
// 	*  }
// 	*  else { do what is written below }
// 	 */

// 	WriteString(payload, msg, EOFSTR, &pos, 0)
// 	logger.GetLogger().Log(logger.Info, "Writing OK packet payload:", payload)
// 	return payload
// }

func OKPacket(length int, specification int, msg string) []byte {
	pLen := 1 + calculateLenEnc(uint64(length)) + calculateLenEnc(uint64(specification))

	payload := make([]byte, pLen)
	pos := 0

	WriteString(payload, "R", FIXEDSTR, &pos, 1)

	WriteLenEncInt(payload, uint64(length), &pos)
	WriteLenEncInt(payload, uint64(specification), &pos)

	WriteString(payload, msg, EOFSTR, &pos, 0)
	return payload

}

// // https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
// func ERRPacket(errcode int, msg string) []byte {
// 	payload := make([]byte, 1+2+len(msg))
// 	pos := 0
// 	// Write ERR packet header
// 	WriteFixedLenInt(payload, INT1, 0xff, &pos)
// 	// Write error code
// 	WriteFixedLenInt(payload, INT2, errcode, &pos)
// 	/* There's one thing to do with client capabilities....that are all ignored
// 	*
// 	*  if capabilities & CLIENT_PROTOCOL_41 { write sql_state_marker string<1> and sql_state string<5>}
// 	 */

// 	// Write human readable error message
// 	WriteString(payload, msg, EOFSTR, &pos, 0)
// 	return payload
// }

// // https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
// func EOFPacket(warnings, status_flags int, capabilities uint32) []byte {
// 	payload := make([]byte, 1)
// 	pos := 0
// 	// Write EOF packet header
// 	WriteFixedLenInt(payload, INT1, 0xfe, &pos)
// 	if Supports(capabilities, CLIENT_PROTOCOL_41) {
// 		// warnings int<2>, status_flags <int2>
// 		WriteFixedLenInt(payload, INT2, warnings, &pos)
// 		WriteFixedLenInt(payload, INT2, status_flags, &pos)
// 	}
// 	return payload
// }

/*---- MISC. FUNCTIONS ---------------------------------------------------------
* Miscellaneous functions that perform common operations. Includes mostly
* arithmetic.
 */

/* min returns the minimum of two functions. */
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

/* Checks bitmask capability flag against server/client/connection capabilities
* and returns true if the bit is set, otherwise false.
 */
func Supports(cflags uint32, c int) bool {
	if (cflags & uint32(c)) != 0 {
		return true
	}
	return false
}

/*  (tentative if this is needed) *******
* Checks that size of slice is enough for the incoming data. */
func checkSize(sz1 int, sz2 int) {
	if sz1 < sz2 {
		log.Fatal(fmt.Sprintf("Array size %d, expected %d", sz1, sz2))
	}
}

func calculateLenEnc(n uint64) int {
	// Determine the length encoded integer.
	l := 1
	if n >= 251 && n < (1<<16) {
		l = 3
	} else if n >= (1<<16) && n < (1<<24) {
		l = 4
	} else if n >= (1 << 24) {
		l = 9
	}
	return l
}

/*---- WRITING BASIC DATA ------------------------------------------------------
* There are three functions. They are mostly useful in writing communication
* packets.
* There are two basic data types in the MySQL protocol: integers and strings.
* An integer is either fixed length or length encoded.
* There are two separate methods for integers.
*
* For strings, there is one method, but you must provide the type of string
* being written as a method argument.
 */

/* Writes an unsigned integer n as a fixed length integer int<l> into the slice
* data. The intptr pos keeps track of where in the buffer (data) we are
* before and after writing to the buffer.
 */
func WriteFixedLenInt(data []byte, l int, n int, pos *int) {
	// Check that the length of data is enough to accomodate the length
	// of the encoding.
	checkSize(len(data[*pos:]), l)
	switch l {
	case INT8:
		data[*pos+7] = byte(n >> 56)
		data[*pos+6] = byte(n >> 48)
		fallthrough
	case INT6:
		data[*pos+5] = byte(n >> 40)
		data[*pos+4] = byte(n >> 32)
		fallthrough
	case INT4:
		// data[*pos+3] = byte(n >> 24)
		data[*pos+3] = byte(n)
		fallthrough
	case INT3:
		data[*pos+2] = byte(n >> 16)
		fallthrough
	case INT2:
		data[*pos+1] = byte(n >> 8)
		fallthrough
	case INT1:
		data[*pos] = byte(n)
	default:
		// if log.V(logger.Warning) {
		//      log.Log(logger.Warning,
		//           fmt.Sprintf("Unexpected fixed int size %d", l))
		// }
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
	l := calculateLenEnc(n)

	// Check that the data byte array is big enough for the desired length.
	// checkSize(len(data[*pos:]), l)

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
		// checkSize(len(data[*pos:]), len(str))
		// Write the string and then terminate with 0x00 byte.
		copy(data[*pos:], str)
		// checkSize(len(data[*pos:]), len(str) + 1)
		*pos += len(str)
		data[*pos] = 0x00
		*pos++

	// case LENENCSTR:
	// 	// Write the encoded length.
	// 	WriteLenEncInt(data, uint64(len(str)), pos)
	// 	// Then write the string as a FIXEDSTR.
	// 	WriteString(data, str, FIXEDSTR, pos, l)

	case FIXEDSTR:

		// checkSize(len(data[*pos:]), l)
		// Pads the string with 0's to fill the specified length l.
		copy(data[*pos:*pos+l], str)
		*pos += l

	case EOFSTR:

		// checkSize(len(data[*pos:]), len(str))
		// Copies the string into the data.
		*pos += copy(data[*pos:], str)
	}
}

/*---- READING DATA ------------------------------------------------------------
* These functions are mostly useful in reading communication packets. There
* are two functions for integer types, and one function for all string types.
 */

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
		n |= uint(data[*pos+7]) << 56
		n |= uint(data[*pos+6]) << 48
		fallthrough
	case INT6:
		n |= uint(data[*pos+5]) << 40
		n |= uint(data[*pos+4]) << 32
		fallthrough
	case INT4:
		n |= uint(data[*pos+3]) << 24
		fallthrough
	case INT3:
		n |= uint(data[*pos+2]) << 16
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
	l := 0 // length of the length encoded integer

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
	default: // 8-byte integer
		return ReadFixedLenInt(data, INT8, pos)
	}
}

/* Reads a string str from the slice data. The method of reading is different
* depending on the string type. l is supposed to be an optional argument
* for when the length needs to specified (i.e. FIXEDSTR (specified), and
* EOFSTR, where the length of the string to be read in is calculated from
* current position and remaining length of packet).
 */
func ReadString(data []byte, stype string_t, pos *int, l int) []byte {
	buf := bytes.NewBuffer(data[*pos:])
	switch stype {
	case NULLSTR:
		line, err := buf.ReadBytes(byte(0x00))
		if err != nil {
			// if log.V(logger.Warning) {
			// 	log.Log(logger.Warning, err)
			// }
			log.Fatal(err)
		}
		*pos += len(line)
		return line

	// case LENENCSTR:
	// 	n := ReadLenEncInt(data, pos)
	// 	if n == 0 {
	// 		break
	// 	}
	// 	buf.ReadByte()
	// 	temp := make([]byte, n)
	// 	n2, err := buf.Read(temp)
	// 	if err != nil {
	// 		// log.Fatal(err)
	// 		// if log.V(logger.Warning) {
	// 		// 	log.Log(logger.Warning, err)
	// 		// }
	// 	} else if n2 != n {
	// 		// if log.V(logger.Warning) {
	// 		// 	log.Log(logger.Warning,
	// 		//            fmt.Sprintf("Read %d, expected %d", n2, n))
	// 		// }
	// 		// log.Fatal(fmt.Sprintf("Read %d, expected %d", n2, n))
	// 	}
	// 	*pos += n
	// 	return temp

	case FIXEDSTR, EOFSTR:
		temp := make([]byte, l)
		n2, err := buf.Read(temp)
		if err != nil {
			// log.Fatal(err)
			// if log.V(logger.Warning) {
			// 	log.Log(logger.Warning, err)
			// }
		} else if n2 != l {
			// if log.V(logger.Warning) {
			// 	log.Log(logger.Warning,
			//            fmt.Sprintf("Read %d, expected %d", n2, l))
			// }
			// log.Fatal(fmt.Sprintf("Read %d, expected %d", n2, l))
		}
		*pos += l
		return temp
	}
	return []byte{}
}
