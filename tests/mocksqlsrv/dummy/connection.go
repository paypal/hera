package dummy

/*
* connection handles communication between the client and server
* through handshake packets, OK/ERR/EOF packets, command packets, and others.
* The functions collected here perform read/writes via the net.Conn
* that a client and server are connected by.
*/

import (
     "net"
     "bufio"
     "log"
     "io"
     "fmt"
     "regexp"
     "strings"
     "strconv"
     "math/rand"
)

/* A Conn represents a connection between the server and client.
* It implements the net.Conn interface and has some helper functions
* and other fields.
*/
type Conn struct {
     conn           net.Conn                 // Client-server connection
     sendBuf        []byte                   // Output buffer
     writeBuf       []byte                   // Write buffer
     sequence_id    int                      // Sequence id (for packets)
     cflags         uint32                   // Shared client-server cflags
     connection_id  int                      // Thread identification number
     useSSLR        bool                     // SSLR true/false
     server_ver     string                   // Server version
     stmtid         int                      // Keeps tracks of number of statement sent
     stmts          map[int]*QueryInfo       // keeps track of queries and stmt ids
     frac           float64                  // probability of random error msg
}

/* Creates a Connection on the server's side. */
func CreateConnection(c net.Conn, config uint32, id int, ver string, frac float64) *Conn {
     // Load all the errcodes and their error description strings into a map.
     Errcodes()

     // Initialize buffers and map for statements
     sendBuf := make([]byte, MAX_PACKET_SIZE)
     writeBuf := make([]byte, MAX_PACKET_SIZE)
     stmts := make(map[int]*QueryInfo)

     return &Conn{conn:c, sendBuf:sendBuf, writeBuf:writeBuf, sequence_id:0,
                    cflags:config, connection_id:id, server_ver:ver, stmtid:0,
                    stmts:stmts, frac:frac}
}

/* Closes connection. */
func (c *Conn) CloseConnection() {
     c.conn.Close()
}

/*=== HANDSHAKE FUNCTIONS ====================================================*/

/* Sends handshake over connection. Only writes Handshakev10 packets. */
func (c *Conn) sendHandshake(p packet_t) {
     scramble := "ham&eggs" // temporary authentication plugin data
     pos := 0
     switch p {
          case HANDSHAKEv10:
               // protocol version
               WriteFixedLenInt(c.writeBuf, INT1, 0xa, &pos)

               // server version
               WriteString(c.writeBuf, c.server_ver, NULLSTR, &pos, 0)

               // thread id
               WriteFixedLenInt(c.writeBuf, INT4, c.connection_id, &pos)

               // Write first 8 bytes of plugin provided data (scramble)
               WriteString(c.writeBuf, scramble, FIXEDSTR, &pos, 8)

               // filler
               WriteFixedLenInt(c.writeBuf, INT1, 0x00, &pos)

               // capability_flags_1
               WriteFixedLenInt(c.writeBuf, INT2, int(c.cflags), &pos)

               // character_set
               WriteFixedLenInt(c.writeBuf, INT1, 0xff, &pos)

               // status_flags
               WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)

               // capability_flags_2
               WriteFixedLenInt(c.writeBuf, INT2, int(c.cflags) >> 16, &pos)

               if isFlagSet(c.cflags, CLIENT_PLUGIN_AUTH) {
                    // authin_plugin_data_len. Temp: 0xaa
                    WriteFixedLenInt(c.writeBuf, INT1, 0xaa, &pos)
               } else {
                    // 00
                    WriteFixedLenInt(c.writeBuf, INT1, 0x00, &pos)
               }
               // reserved
               WriteString(c.writeBuf, strings.Repeat("0", 10), FIXEDSTR, &pos, 10)

               // auth-plugin-data-part-2
               WriteString(c.writeBuf, scramble, LENENCSTR, &pos, 13)

               if isFlagSet(c.cflags, CLIENT_PLUGIN_AUTH) {
                    plugin_name := "temp_auth"
                    WriteString(c.writeBuf, plugin_name, NULLSTR, &pos, 0)
               }
          default:
               log.Fatal("Unsupported handshake version")

     }
     c.WritePacket(c.writeBuf[0:pos], pos)
}

/* Receives handshake from connection and takes bitwise ANDs the
* capability flags of client and server. */
func (c *Conn) receiveHandshakeResponse(packet []byte, packetLen uint32) {
     pos := 0  // index tracker
     if !isFlagSet(c.cflags, CLIENT_PROTOCOL_41) {

          // log : Reading HANDSHAKE_RESPONSE_320
          // lflags := ReadFixedLenInt(packet, INT2, &pos)
          // mpsize := ReadFixedLenInt(packet, INT3, &pos)
          ReadFixedLenInt(packet, INT2, &pos)
          ReadFixedLenInt(packet, INT3, &pos)

          // Username (null-terminated string)
          // user := ReadString(packet, NULLSTR, &pos, 0)
          ReadString(packet, NULLSTR, &pos, 0)

          if isFlagSet(c.cflags, CLIENT_CONNECT_WITH_DB) {
               // auth_response := ReadString(packet, NULLSTR, &pos, 0)
               ReadString(packet, NULLSTR, &pos, 0)
               // dbname := ReadString(packet, NULLSTR, &pos, 0)
               ReadString(packet, NULLSTR, &pos, 0)
          } else {
               // auth_response := ReadString(packet, EOFSTR, &pos, int(packetLen) - pos)
               ReadString(packet, EOFSTR, &pos, int(packetLen) - pos)
          }
     } else {
          // log : Reading HANDSHAKE_RESPONSE_41

          // client flags
          flags := uint32(ReadFixedLenInt(packet, INT4, &pos))
          c.cflags &= flags

          // maximum packet size, 0xFFFFFF max
          // mpsize := ReadFixedLenInt(packet, INT4, &pos)
          ReadFixedLenInt(packet, INT4, &pos)

          // character set
          ReadFixedLenInt(packet, INT1, &pos)

          // filler string
          ReadString(packet, FIXEDSTR, &pos, 23)

          // username
          // user := ReadString(packet, NULLSTR, &pos, 0)
          ReadString(packet, NULLSTR, &pos, 0)

          if isFlagSet(c.cflags, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
               // auth_response := ReadString(packet, LENENCSTR, &pos, 0)
               ReadString(packet, LENENCSTR, &pos, 0)
          } else {
               // auth_response_length := ReadFixedLenInt(packet, INT1, &pos)
               n := ReadFixedLenInt(packet, INT1, &pos)

               ReadString(packet, FIXEDSTR, &pos, n)
          }

          if isFlagSet(c.cflags, CLIENT_CONNECT_WITH_DB) {
               // dbname := ReadString(packet, NULLSTR, &pos, 0)
               ReadString(packet, NULLSTR, &pos, 0)
          }

          if isFlagSet(c.cflags, CLIENT_PLUGIN_AUTH) {
               // client_plugin_name := ReadString(packet, NULLSTR, &pos, 0)
               ReadString(packet, NULLSTR, &pos, 0)
          }

          if isFlagSet(c.cflags, CLIENT_CONNECT_ATTRS) {
               // key_val_len := ReadLenEncInt(packet, &pos)
               ReadLenEncInt(packet, &pos)
          }

     }
}



/*=== WRITING PACKETS ========================================================*/

/*
* Given a packet type, write the packet so that it's ready to be sent
* through the Conn. The contents are written into the field in the conn,
* which is a shared buffer between all functions.
*
* All packets in the MySQL protocol follow this structure:
*
*       3 bytes (int<3>)      1 byte (int<1>)          n bytes

*    |   payload_length   |      sequence_id      |    payload    |
*    |------------------ header ------------------|
*
* such that n does not exceed 2^24 - 1.
*
* writePacket receives a payload from functions like send...Packet()
* and prepends the header and writes it out to the connection using
* the Conn writer. writePacket assumes either a com_query_response packet,
* a resultset, or connection phase packets sent from the server's side.
*/
func (c *Conn) WritePacket(payload []byte, pos int) {
     /* Set current payload length and tracking index. */
     length := pos
     idx := 0
     pidx := 0
     for length > 0 {
          /* Determine packetLength which is capped by MAX_PACKET_SIZE. */
          packetsize := min(length, int(MAX_PACKET_SIZE))

          /* Encode the packetLength into the header. */
          WriteFixedLenInt(c.sendBuf, INT3, packetsize, &idx)
          WriteFixedLenInt(c.sendBuf, INT1, c.sequence_id, &idx)

          // Copy over the data to the send buffer.
          n := copy(c.sendBuf[HEADER_SIZE : HEADER_SIZE + packetsize],
                         payload[pidx:packetsize])

          pidx += n

          if n != packetsize {
               log.Fatal(fmt.Sprintf("Wrote %d bytes, expected %d", n, packetsize))
          }

          idx += packetsize

          /* Write the packet out to the connection. */
          i, err := c.conn.Write(c.sendBuf[:packetsize + HEADER_SIZE])

          if err != nil {
               log.Fatal(err)
          } else if i != packetsize + HEADER_SIZE {
               log.Fatal(fmt.Sprintf("Wrote %d bytes"))
          }

          /* Update remaining payload length and sequence_id. */
          length -= packetsize
          c.sequence_id++

     }
}


/* Sends OK Packet. */
func (c *Conn) sendOKPacket(msg string) {
     pos := 0

     // Write header int (1 byte)
     WriteFixedLenInt(c.writeBuf, INT1, 0x00, &pos)

     // Write affected_rows (lenenc int). Temp: 0
     WriteLenEncInt(c.writeBuf, 0x00, &pos)

     // Write last_insert_id (lenenc int). Temp: 0
     WriteLenEncInt(c.writeBuf, 0x00, &pos)

     if isFlagSet(c.cflags, CLIENT_PROTOCOL_41) {
          // Write status flags. Temp: 0x00
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)

          // Write number of warnings. Temp: 0x00
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     } else if isFlagSet(c.cflags, CLIENT_TRANSACTIONS) {
          // Write status flags. Temp: 0x00
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     }

     // TODO: Unsure what information goes below here.
     if isFlagSet(c.cflags, CLIENT_SESSION_TRACK) {
          info := "temp stuff"
          WriteString(c.writeBuf, info, LENENCSTR, &pos, 0)

     }
     status_flags := uint32(0) // temporary throwaway value
     SERVER_SESSION_STATE_CHANGED := cflag(0) // temporary throwaway value
     // No status flags, but this would have been in packet had there been:
     if isFlagSet(status_flags, SERVER_SESSION_STATE_CHANGED) {
          WriteString(c.writeBuf, "temp stuff", LENENCSTR, &pos, 0)
     } else {
          WriteString(c.writeBuf, "infoEOF", LENENCSTR, &pos, 0)
     }

     c.WritePacket(c.writeBuf[0:pos], pos)
}


/* Sends ERR Packet. */
func (c *Conn) sendERRPacket(errc errcode, msg string) {
     c.sendIntERRPacket(int(errc), msg)
}
func (c *Conn) sendIntERRPacket(errc int, msg string) {
     pos := 0

     // Write header int (1 byte).
     WriteFixedLenInt(c.writeBuf, INT1, 0xff, &pos)
     // Write error code (2 bytes).
     WriteFixedLenInt(c.writeBuf, INT2, int(errc), &pos)

     if isFlagSet(c.cflags, CLIENT_PROTOCOL_41) {
          // Write SQL state marker '#' (1 byte)
          // Write sql_state (5 bytes)
     }

     // Write error message into the packet.
     WriteString(c.writeBuf, msg, EOFSTR, &pos, 0)

     c.WritePacket(c.writeBuf[0:pos], pos)
}

/* Sends EOF packet. */
func (c *Conn) sendEOFPacket() {
     pos := 0
     // Write header byte (1 byte)
     WriteFixedLenInt(c.writeBuf, INT1, 0xfe, &pos)

     if isFlagSet(c.cflags, CLIENT_PROTOCOL_41) {
          // Write number of warnings. Temp: 0
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
          // Write status flags. Temp: 0xffff
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     }
     c.WritePacket(c.writeBuf[0:pos], pos)
}

/* Writes Column Definition packet.
* https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
*/
func (c *Conn) writeColumnDefinition(cmd COM, qi *QueryInfo) {
     // write it into the sendBuf, so that PreparedRSPacket will
     // be fully contained in writeBuf

     // For each column defined in the query info struct, write out its
     // column definition.
     for _, co := range qi.colNames {
          pos := 0
          // write catalog
          WriteString(c.writeBuf, "def", LENENCSTR, &pos, 0)
          // write schema
          WriteString(c.writeBuf, strings.Join(qi.colNames, ","), LENENCSTR, &pos, 0)
          // write table
          WriteString(c.writeBuf, "jdbc_hera_test", LENENCSTR, &pos, 0)
          // write org_table
          WriteString(c.writeBuf, "mysql.jdbc_hera_test", LENENCSTR, &pos, 0)
          // write name
          WriteString(c.writeBuf, co, LENENCSTR, &pos, 0)
          // write org_name
          WriteString(c.writeBuf, "jdbc_hera_test." + co, LENENCSTR, &pos, 0)
          // write length of fixed-length fields [0c]
          WriteLenEncInt(c.writeBuf, 0x0c, &pos)
          // character set
          WriteFixedLenInt(c.writeBuf, INT2, 0x2d, &pos)

          // column length
          WriteFixedLenInt(c.writeBuf, INT4, 0xffff, &pos)

          // type
          WriteFixedLenInt(c.writeBuf, INT1, 0xfe, &pos)

          // flags
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)

          // decimals;
          // 0x00 for integers and static strings,
          // 0x1f for dyna,oc strings, double, and float;
          // 0x00 to 0x51 for decimals
          WriteFixedLenInt(c.writeBuf, INT1, 0x00, &pos)

          // filler
          WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)

          /*
          if cmd == COM_FIELD_LIST {
               dv := "foobar"
               m := len(dv)
               WriteLenEncInt(c.writeBuf, uint64(m), &pos)
               WriteString(c.writeBuf, dv, FIXEDSTR, &pos, m)
          }
          */

          c.WritePacket(c.writeBuf[0:pos], pos)
     }
}

/* Writes COM_STMT_PREPARE_OK (response) packet.
*
*  https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
*/
func (c *Conn) sendPreparedRSPacket(qi *QueryInfo) {
     c.sendPreparedRSPacketBad(qi, 0x00)
}
func (c *Conn) sendPreparedRSPacketBad(qi *QueryInfo, errI int) {
     pos := 0

     // Write status int (1 byte).
     WriteFixedLenInt(c.writeBuf, INT1, errI, &pos)
     // Write statement id (4 bytes).
     WriteFixedLenInt(c.writeBuf, INT4, c.stmtid, &pos)
     // Write number of columns (1 byte).
     WriteFixedLenInt(c.writeBuf, INT2, qi.numColumns, &pos)
     // Write num_params (2 bytes).
     WriteFixedLenInt(c.writeBuf, INT2, qi.numParams, &pos)

     // Write reserved filler (1 byte)
     WriteFixedLenInt(c.writeBuf, INT1, 0x00, &pos)
     // Write number of warnings (2 bytes)
     WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     c.WritePacket(c.writeBuf[0:pos], pos)



     // Write column definition packet if there are packets to follow.
     if qi.numParams > 0 {
          c.writeColumnDefinition(COM_STMT_PREPARE, qi)
          // Send EOF packet to indicate end.
          c.sendEOFPacket()
     }

     if qi.numColumns > 0 {
          c.writeColumnDefinition(COM_STMT_PREPARE, qi)
          // Send EOF packet to indicate end.
          c.sendEOFPacket()
     }

}


/* Writes out a binary protocol resultset packet to the write buffer.
*    https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
* NOTE*****!
* Since this server doesn't actually connect to a storage engine, the
* binary protocol result set packet that is sent out is always the same,
* and is always a single row from a single column projection, with string
* "foobar".
*/
func (c *Conn) sendBinProtRS(qi *QueryInfo) {
     pos := 0

     // Write column_count (which is currently default 1).
     WriteLenEncInt(c.writeBuf, 0x01, &pos)
     c.WritePacket(c.writeBuf[0:pos], pos)

     // column definition
     pos = 0
     WriteFixedLenInt(c.writeBuf, INT4, 0x66656403, &pos)
     WriteFixedLenInt(c.writeBuf, INT3, 0x00, &pos)
     WriteFixedLenInt(c.writeBuf, INT4, 0x6c6f6304, &pos)
     WriteFixedLenInt(c.writeBuf, INT4, 0x080c0031, &pos)
     WriteFixedLenInt(c.writeBuf, INT2, 0600, &pos)
     WriteFixedLenInt(c.writeBuf, INT3, 0x00, &pos)
     WriteFixedLenInt(c.writeBuf, INT3, 0xfd, &pos)
     WriteFixedLenInt(c.writeBuf, INT3, 0x1f, &pos)
     WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     c.WritePacket(c.writeBuf[0:pos], pos)

     // send EOF packet to indicate end of column definition packets
     c.sendEOFPacket()

     // result set row
     pos = 0
     WriteFixedLenInt(c.writeBuf, INT2, 0x00, &pos)
     WriteFixedLenInt(c.writeBuf, INT4, 0x6f6f6606, &pos)
     WriteFixedLenInt(c.writeBuf, INT3, 0x726162, &pos)
     c.WritePacket(c.writeBuf[0:pos], pos)

     // send EOF packet to indicate end of result set rows
     c.sendEOFPacket()
}




/*=== RECEIVING PACKETS ======================================================*/

/*
* Reads packet which was obtained from the connection's reader buffer.
* As of right now, it just returns a bool indicating whether or not the
* received packet is the client continuing the connection, Returning 'true'
* implies the handler should keep receiving packets from the client.
* This is used for processing the header of incoming packets, passing
* the payload to receiveHandshakeResponse if necessary, or
* working through the command packet.
*/
func (c *Conn) readPacket(handshake bool) (bool) {

     reader := bufio.NewReader(c.conn)
     // Read in the header and sequence id of the packet.
     a, err := reader.ReadByte()
     b, err := reader.ReadByte()
     d, err := reader.ReadByte()
     length := uint32(d) << 16 | uint32(b) << 8 | uint32(a)

     // Increase the sequence id by 1 because a packet was just received
     // from the client.
     _, err = reader.ReadByte()
     c.sequence_id++

     // Read in the payload.
     payload := make([]byte, length)
     n, err := io.ReadFull(reader, payload)

     // Check that the length of the payload is correct.
     if n != int(length) {
          log.Fatal(fmt.Sprintf("Expected %d bytes, read %d", length, n))
     } else if err != nil {
          log.Fatal(err)
     }

     // If this is expected to be a handshake packet, then leave
     // the rest to the handshakeresponse function.
     if handshake {
          c.receiveHandshakeResponse(payload, length)

     } else {
          // If the length of the payload is 0, then stop the
          // loop in the handler. Otherwise continue on.
          if length <= 0 { return false }

          // This is a command packet, so process according to
          // what kind of command it is.
          payloadStr := string(payload[1:])
          switch COM(payload[0]) {

               case COM_QUERY:
                    // If this has a select statement, send rows, otherwise OK
                    if strings.HasPrefix(strings.ToLower(payloadStr), "select") {
                         c.sendBinProtRS(&QueryInfo{})
                    } else {
                         // frac% failure, sending err packet instead
                         r := rand.Float64()
                         if r <= c.frac {
                              c.RandomError()
                              return false
                         }
                         c.sendOKPacket("Got it!")
                    }

               case COM_STMT_PREPARE:
                    // New statement; increment stmtid
                    c.stmtid++
                    // anything set with prepare
                    // Create new mapping between stmt id and actual statement
                    qi := ParseQuery(payloadStr, c.stmtid)
                    c.stmts[c.stmtid] = qi

                    /* This implies the rest of the payload is the query. */

                    re,_ := regexp.Compile(`mockErr([0-9]*)`)
                    sub := re.FindStringSubmatch(payloadStr)
                    if len(sub) > 0 {
                         errI, _ := strconv.Atoi(string(sub[1]))
                         c.sendIntERRPacket(errI, "MockEr-p-fixed-msg")
                    } else {
                         /* Just return a COM_STMT_PREPARE_OK. */
                         c.sendPreparedRSPacket(qi)
                    }

               case COM_STMT_EXECUTE:

                    // Execute a statement (could have been statement)
                    pos := 0
                    // Read in the statement id
                    id := ReadFixedLenInt(payload[1:], INT4, &pos)

                    // Get the queryinfo struct from the stmts map
                    queryinfo := c.stmts[id]

                    // This is so that the unit tests doesn't fail on the mysqlworker adapter
                    if (*queryinfo).query != "select @@global.read_only" {
                         // frac% failure which sends err packet instead.
                         r := rand.Float64()
                         if r < c.frac {
                              c.RandomError()
                              return false
                         }
                    }

                    // Check if select; return rows, otherwise send OK
                    if strings.Contains(strings.ToLower((*queryinfo).query), "select") {
                         c.sendBinProtRS(queryinfo)
                    } else {
                         c.sendOKPacket("Got it!")
                    }

               case COM_STMT_CLOSE:
                    // Get the statement id and remove it from the map.
                    pos := 0
                    id := ReadFixedLenInt(payload[1:], INT4, &pos)
                    delete(c.stmts, id)

                    // Does not send any response to the client.

               // Other commands.
               case COM_QUIT:
                    return false
          }

     }

     // Return true indicates that the handler should keep receiving commands.
     return true
}

/* Resets the sequence_id and has readPacket obtain the information
* from the buffer. */
func (c *Conn) receiveCommand() bool {
     // Reset sequence-id when a new command begins in the command phase.
     c.sequence_id = 0

     // Read from connection into buffer.
     return c.readPacket(false)

}


/*=== RAISING ERRORS =========================================================*/
// These functions are guaranteed to make the client report an error!

/* Sends a random error back to the client in response to a packet.
* Any of the common MySQL error codes and messages defined in errors.go.
*/
func (c *Conn) RandomError() {
     code := codes[rand.Intn(len(errs))]
     c.sendERRPacket(code, errs[code])
}
