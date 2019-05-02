# Driver properties

As a JDBC driver, the client is configurable thorugh a java.util.Properties object passed in to java.sql.DriverManager.getConnection() when creating a connection.

The client application is not required to provide values for the driver properties when creating a connection. For reference, this is the list of the properties than can be used. They are defined as constants in the OCCClientConfigHolder class or the OccConnectionConfig class which are part of the com.paypal.jmux.conf package.


## occ.support.column_names
+ It is used internally in order to implement the ResultSet methods using the column names instead of column position, for example ResultSet.getString(String). If it is false and the application tries to use ResultSet with column names it will throw an exception. When true, there is a minor performance penalty the first time a query is run. The assumption is that for a certain query the column list and their order is not changed
+ default: true
+ constant: OCCClientConfigHolder.SUPPORT_COLUMN_NAMES_PROPERTY

## occ.support.column_info
+ It is used internally in order to implement ResultSetMetaData
+ default: true
+ constant: OCCClientConfigHolder.SUPPORT_COLUMN_INFO_PROPERTY

## occ.support.rs_metadata
+ If it is true the ResultSetMetaData is supported
+ default: true
+ constant: OCCClientConfigHolder.SUPPORT_RS_METADATA_PROPERTY

## occ.min_fetch_size
+ It is used by Statement.setFetchSize, this is the minimum fetch size that can be set
+ default: 2
+ constant: OCCClientConfigHolder.MIN_FETCH_SIZE_PROPERTY

## occ.connection.factory
+ The factory class used to create a java.sql.Connection
+ default: com.paypal.jmux.conn.OccTCPConnectionFactory
+ constant: OCCClientConfigHolder.CONNECTION_FACTORY_PROPERTY

## occ.response.timeout.ms
+ It is the maximum time in milliseconds to wait for a response from the server after a request was sent
+ default: 60000
+ constant: OCCClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY

## occ.enable.escape
+ If true then it enables JDBC escaping. Note: only escape call is implemented
+ default: true
+ constant: OCCClientConfigHolder.ENABLE_ESCAPE_PROPERTY

## occ.enable.sharding
+ If true than it enables the sharding functionality
+ default: false
+ constant: OCCClientConfigHolder.ENABLE_SHARDING_PROPERTY

## occ.enable.param_name_binding
+ If it is true then the name of the bind parameters is the same as the corresponding column name. It is useful for sharding, so the server can find the shard key value.
+ default: true
+ constant: OCCClientConfigHolder.ENABLE_PARAM_NAME_BINDING

## occ.db_encoding.utf8
+ It is true if the database encoding is UTF-8
+ default: true
+ constant: OCCClientConfigHolder.DB_ENCODING_UTF8

## occ.connection.retries
+ It is the number of times to retry a connection attempt
+ default: 1
+ constant: OccConnectionConfig.CONNECTION_RETRIES_PROPERTY

## occ.connection.timeout.msecs
+ The timeout waiting for the connection to be established
+ default: 7000
+ constant: OccConnectionConfig.CONNECTION_TIMEOUT_MSECS_PROPERTY

## occ.socket.sendbuffer
+ The size of the socket buffer for write. 0 means the value is not set, so it will use the system default
+ default: 0
+ constant: OccConnectionConfig.SO_SENDBUFFER_PROPERTY

## occ.socket.receivebuffer
+ The size of the socket buffer for read. 0 means the value is not set, so it will use the system default
+ default: 0
+ constant: OccConnectionConfig.SO_RECEIVEBUFFER_PROPERTY

## occ.socket.tcpnodelay
+ The TCP_NODELAY option to be set on the socket
+ default: true
+ constant: OccConnectionConfig.TCP_NO_DELAY_PROPERTY
