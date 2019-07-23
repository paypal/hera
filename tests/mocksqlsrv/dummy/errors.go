package dummy

/* This information packet contains the error code, SQL state, and
* description of the error. As of right now, it's defined but not 
* used because the ERR Packet sending function does not support 
* writing the SQL state.
*/
type errpack struct {
     code  errcode            // error code
     SQLstate string          // sql state
     dscr string              // description of the error
}

var errs map[errcode]string
var codes []errcode

/* Common SQL errcodes received by client from server */
type errcode int
const (
     ER_DISK_FULL                            errcode = 1021
     ER_GET_ERRN                             errcode = 1030
     ER_OUTOFMEMORY                          errcode = 1037
     ER_OUT_OF_SORTMEMORY                    errcode = 1038
     ER_UNEXPECTED_EOF                       errcode = 1039
     ER_CON_COUNT_ERROR                      errcode = 1040
     ER_OUT_OF_RESOURCES                     errcode = 1041
     ER_SERVER_SHUTDOWN                      errcode = 1053
     ER_NORMAL_SHUTDOWN                      errcode = 1077
     ER_GOT_SIGNAL                           errcode = 1078
     ER_SHUTDOWN_COMPLETE                    errcode = 1079
     ER_FORCING_CLOSE                        errcode = 1080
     ER_STACK_OVERRUN                        errcode = 1119
     ER_HOST_IS_BLOCKED                      errcode = 1129
     ER_HOST_NOT_PRIVILEGED                  errcode = 1130
     ER_ABORTING_CONNECTION                  errcode = 1152
     ER_NET_PACKET_TOO_LARGE                 errcode = 1153
     ER_NET_READ_ERROR_FROM_PIPE             errcode = 1154
     ER_NET_FCNTL_ERROR                      errcode = 1155
     ER_NET_PACKETS_OUT_OF_ORDER             errcode = 1156
     ER_NET_UNCOMPRESS_ERROR                 errcode = 1157
     ER_NET_READ_ERROR                       errcode = 1158
     ER_NET_READ_INTERRUPTED                 errcode = 1159
     ER_NET_ERROR_ON_WRITE                   errcode = 1160
     ER_NET_WRITE_INTERRUPTED                errcode = 1161
     ER_NEW_ABORTING_CONNECTION              errcode = 1184
     ER_TOO_MANY_USER_CONNECTIONS            errcode = 1203
     ER_QUERY_INTERRUPTED                    errcode = 1317
     ER_READ_ONLY_MODE                       errcode = 1836
     ER_MUST_CHANGE_PASSWORD_LOGIN           errcode = 1862
     ER_ACCESS_DENIED_CHANGE_USER_ERROR      errcode = 1873
     ER_INNODB_READ_ONLY                     errcode = 1874
     ER_TEMP_FILE_WRITE_FAILURE              errcode = 1878
)

/* Compiles errcodes into map between errcode and string. */
func Errcodes() (map[errcode]string, []errcode) {

     errs = make(map[errcode]string)

     descrs := []string{"Disk full (%s); waiting for someone to free some space...",
          "Got error %d from storage engine",
          "Out of memory; restart server and try again (needed %d bytes)",
          "Out of sort memory, consider increasing server sort buffer size",
          "Unexpected EOF found when reading file '%s' (Errno: %d)",
          "Too many connections",
          "Out of memory; check if mysqld or some other process uses all available memory; if not, you may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space",
          "Server shutdown in progress",
          "%s: Normal shutdown",
          "%s: Got signal %d. Aborting!",
          "%s: Shutdown complete",
          "%s: Forcing close of thread %ld user: '%s'",
          "Thread stack overrun: Used: %ld of a %ld stack. Use 'mysqld --thread_stack=#' to specify a bigger stack if needed",
          "Host '%s' is blocked because of many connection errors; unblock with 'mysqladmin flush-hosts'",
          "Host '%s' is not allowed to connect to this MySQL server",
          "Aborted connection %ld to db: '%s' user: '%s' (%s)",
          "Got a packet bigger than 'max_allowed_packet' bytes",
          "Got a read error from the connection pipe",
          "Got an error from fcntl()",
          "Got packets out of order",
          "Couldn't uncompress communication packet",
          "Got an error reading communication packets",
          "Got timeout reading communication packets",
          "Got an error writing communication packets",
          "Got timeout writing communication packets",
          "Aborted connection %ld to db: '%s' user: '%s' host: '%s' (%s)",
          "User %s already has more than 'max_user_connections' active connections",
          "Query execution was interrupted",
          "Running in read-only mode",
          "Your password has expired. To log in you must change it using a client that supports expired passwords.",
          "Access denied trying to change to user '%s'@'%s' (using password: %s). Disconnecting.",
          "InnoDB is in read only mode.",
          "Temporary file write failure."}

     codes = []errcode{ER_DISK_FULL,
          ER_GET_ERRN,
          ER_OUTOFMEMORY,
          ER_OUT_OF_SORTMEMORY,
          ER_UNEXPECTED_EOF,
          ER_CON_COUNT_ERROR,
          ER_OUT_OF_RESOURCES,
          ER_SERVER_SHUTDOWN,
          ER_NORMAL_SHUTDOWN,
          ER_GOT_SIGNAL,
          ER_SHUTDOWN_COMPLETE,
          ER_FORCING_CLOSE,
          ER_STACK_OVERRUN,
          ER_HOST_IS_BLOCKED,
          ER_HOST_NOT_PRIVILEGED,
          ER_ABORTING_CONNECTION,
          ER_NET_PACKET_TOO_LARGE,
          ER_NET_READ_ERROR_FROM_PIPE,
          ER_NET_FCNTL_ERROR,
          ER_NET_PACKETS_OUT_OF_ORDER,
          ER_NET_UNCOMPRESS_ERROR,
          ER_NET_READ_ERROR,
          ER_NET_READ_INTERRUPTED,
          ER_NET_ERROR_ON_WRITE,
          ER_NET_WRITE_INTERRUPTED,
          ER_NEW_ABORTING_CONNECTION,
          ER_TOO_MANY_USER_CONNECTIONS,
          ER_QUERY_INTERRUPTED,
          ER_READ_ONLY_MODE,
          ER_MUST_CHANGE_PASSWORD_LOGIN,
          ER_ACCESS_DENIED_CHANGE_USER_ERROR,
          ER_INNODB_READ_ONLY,
          ER_TEMP_FILE_WRITE_FAILURE}

     for i, v := range(codes) {

          errs[v] = descrs[i]
     }

     return errs, codes
}
