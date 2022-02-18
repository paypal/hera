# Configuration

The user name, password and the data source information are set via environment variables. All other configuration parameters are defined in hera.txt configuration file.

## Environment variables

### username

This is the database user name.

### password

This is the database password

### TWO_TASK

This is the data source information for the MySQL, Oracle or PostgreSQL database. 

### Oracle
For **Oracle** the format can be in the form of `(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)(PORT=port)))(CONNECT_DATA=(SERVICE_NAME=sn)))`. Or it can be a name of an entry in `tnsnames.ora`. Please see the Oracle documentation for more details.

We use the same environment name for MySQL and PostgreSQL. 

### MySql
For **MySQL**, the value can be *tcp(127.0.0.1:3306)/myschema*. 

Failover uses two pipes to separate entries -> `tcp(127.0.0.1:3306)/myschema?timeout=9s||tcp(127.0.0.2:3306)/myschema`. 

Set environment variable `certdir` to load all the pem files that you can specify as certificate authorities for the mysql worker to accept.

### PostgreSQL
For **PostgreSQL**, the format can be *host:port/dbName?paramspec*. 

For example, `127.0.0.1:5431/postgres?connect_timeout=60&sslrootcert=postgres-ca.pem&sslmode=verify-ca`.

PostgreSQL failover also uses two pipes to separate entries -> `127.0.0.1:5431/postgres?connect_timeout=60||127.0.0.2:5431/postgres?connect_timeout=60`.

### Sharding
For **sharding** case, we need to define multiple datasources, one for each shard. The convention is to define the datasource for the first shard in `TWO_TASK_0` environment variable, for the second shard in `TWO_TASK_1`, etc.

### TAF
For TAF (Transparent Application Failover), the data source information for the fallback database is in `TWO_TASK_STANDBY0` environment variable. If we have multiple shards, then the environment variable are `TWO_TASK_STANDBY0_0` (first shard), `TWO_TASK_STANDBY0_1`, etc.

### Read Write Split
For R/W split, the data source information for a read node is in `TWO_TASK_READ` environment variable. If we have multiple shards, then the environment variable are `TWO_TASK_READ_0` (first shard), `TWO_TASK_READ_1`, etc.

### LD_LIBRARY_PATH

The Oracle worker uses the Oracle instant client shared libraries, so LD_LIBRARY_PATH needs to contain the location of those libraries.

## hera.txt entries

There are two types of configuration parameters: **static parameters** and **dynamic parameters**. The static parameters are loaded at the application startup and stay fixed until the process shuts down. The dynamic parameters are re-loaded periodically. Their name is prefixed with 'opscfg.hera.server.'

### Static parameters

#### bind_port
+ The TCP port listening for incoming connections requests
+ it is a required parameter

#### log_file
+ The file name where the logs are written
+ default: hera.log

#### log_level
+ The severity of the messages that are displayed.
+ values: 0 (alert), 1 (warning), 2 (info), 3 (debug), 4 (verbose)
+ default: 2

#### key_file
+ The file name of the RSA key file used to configure as TLS server. If unset, the server uses plain TCP instead of TLS.
+ default: ""

#### cert_chain_file
+ The name of the file containing the certificates chain
+ default: ""

#### lifo_scheduler_enabled
+ Defines the policy for alocating worker to perform SQLs. If this value is true, the scheduling is LIFO (last in - first out) which means when a worker is released it is put at the top of the free list and it will be the first to be allocated. LIFO is generaly better because it makes a better use of the database caching. If this value is false, the scheduling is FIFO, basically alocating the workers in a round-robin fashion.
+ default: true

#### config_reload_time_ms
+ The interval in milliseconds at which the dynamic configuration is reloaded
+ default: 30000

#### max_stranded_time_interval
+ The timeout in milliseconds to wait for a worker to cancel a query in progress. If the timeout expires then the worker is recycled.
+ default: 2000

#### state_log_interval
+ The interval in seconds to write the worker state.
+ default: 1

#### database_type
+ The type of database to connect.
+ default: oracle

#### enable_sharding
+ If the value is 'true' then sharding is enabled
+ default: false

#### use_shardmap
+ If the value is 'true', the application will load the sharding configuration from a table. This configuration mainly contains the mapping from bucket to shard. Having this value false is usefull for development / testing.
+ default: true.

#### num_shards
+ The number of shards
+ default: 1

#### shard_key_name
+ The name of the shard key
+ default: ""

#### max_scuttle
+ The number of buckets (scuttles). Must be between 1 and 1024.
+ default: 1024

#### scuttle_col_name
+ The name on the scuttle ID column
+ default: scuttle_id

#### sharding_algo
+ The algoritm defining how to map a shard key to a scuttle ID. Currently we support two algorithms: "HASH" and "MOD". For HASH, the scuttle ID is the remainder of dividing the  MurmurHash of the key by the number of scuttles. MOD is suitable for number columns with distinct values (like an primary key with auto-increments), the scuttle ID is the remainder dividing the value of the key by the number of scuttles.
+ default: "HASH"

#### sharding_postfix
+ If it is empty / not defined then the table for loading the shard map is "<<management_table_prefix>>_shard_map" otherwise is "<<management_table_prefix>>_shard_map_<<sharding_postfix>>".
+ default: ""

#### enable_whitelist_test
+ It enables the whitelist testing mode.
+ default: false

#### whitelist_children
+ When whitelist test is enabled, it is the number of workers for all the shards except the first shard. This is usefull for transitioning an existing application from one shard to multiple shards.
+ default: 5

#### sharding_cfg_reload_interval
+ The interval in seconds the shard map is reloaded
+ default: 2

#### sharding_cross_keys_err
+ If it is "true" then it will return an error if the client is attempting a query in the same shard but with a different key than the key used in the earlier query which started a transaction. If it is "false" than it will log and continue.
+ default: false

#### shard_key_value_type_is_string
+ This is to indicate the type of the shard value. If the shard key value is a string, it is set to "true"
+ default: false

#### management_table_prefix
+ The prefix for the managament tables: shard map config table (hera_shard_map), rac maintenance config table (hera_maint), rate limiter config table (hera_rate_limiter)
+ default: "hera"

#### rac_sql_interval
+ The interval, in seconds, to check if RAC maintenance was done and the worker need restarted.
+ default: 10

#### rac_restart_window
+ When the workers need to be restarted this is done gradualy over a window. This configuration defines the interval in seconds where the restarts are spred out.
+ default: 240

#### lifespan_check_interval 
+ The interval, in seconds, to check if the workers lifespan has expired and they need to be recycled.
+ default: 10

#### enable_query_bind_blocker
+ This is in addition to the automatic bind eviction. It helps the DBAs to block/throttle a SQL with a specific bind variable value. Database will have a table name called <<management_table_prefix>>_rate_limiter,  For sharded databases, this needs to be on sh0 and will block queries headed to any shard.
+ default: false

#### query_bind_blocker_min_sql_prefix
+ SQLs with sqltext under this length will not be considered for rate limiting.
+ default: 20

#### enable_taf
+ It it is "true" then Transparent Application Failover (i.e. TAF) feature is enabled.
+ default: false

#### taf_timeout_ms
+ In case of TAF (Transparent Application Failover), it is the timeout in milliseconds to wait for a query to complete before it is canceled to be re-tried on the falback database
+ default: 200

#### readonly_children_pct
+ If R/W split is enabled this is the percentage of workers connecting to a read node.
+ default: 0

#### backlog_pct
+ Defines the backlog queue fill percentage threshold for the saturation recovery to start in order help with the backlog.
+ default: 30

#### request_backlog_timeout
+ The backlog timeout to be used if the backlog queue is empty
+ default: 1000

#### short_backlog_timeout
+ The backlog timeout to be used if the backlog queue is not empty
+ default: 30

#### soft_eviction_effective_time
+ The period of time a SQL is blacklisted
+ default: 10000

#### soft_eviction_probability
+ The probability to do soft eviction
+ default: 50

#### bind_eviction_threshold_pct
+ A bind name+value is evicted when mux is overloaded and they occupy over bind_eviction_threshold_pct.
 + default: 25

#### bind_eviction_target_conn_pct
+ A bind name+value is evicted when mux is overloaded and they occupy over bind_eviction_threshold_pct. Their queries get blocked more when more than bind_eviction_target_conn_pct are busy or in wait state.
+ default: 50

#### bind_eviction_decr_per_sec
+ If the evicted query hasn't been seen for a while, the level of blocking is reduced by this value.
+ default: 1.0

#### bouncer_enabled
+ Enables bouncing connections when the number of open connections crosses the threshold 
+ default: true

#### bouncer_startup_delay
+ The delay in seconds before the bouncer actually starts bouncing connections. This is to avoid bouncing in case of short lived bursts 
+ default: 10

#### bouncer_poll_interval_ms
+ The bouncing condition needs to be re-confirmed 4 times after <<bouncer_poll_interval_ms>> milliseconds before the bouncer is actually activated
+ default: 100

#### mux_pid_file
+ The file name containing the process ID.
+ default: mux.pid

#### error_code_prefix
+ The prefix to be added for all the error codes.
+ default: HERA

#### state_log_prefix
+ The prefix to be added for the state log entries.
+ default: hera

#### enable_danglingworker_recovery
+ If it is "true" it will terminate workers that are allocated for a long period, three times the idle timeout. 
+ default: false

#### go_stats_interval
+ The interval to print statistics to CAL
+ default: 20

### Dynamic parameters

#### opscfg.hera.server.log_level
+ The log severity level
+ default: the value of the static "log_level" entry

#### opscfg.hera.server.idle_timeout_ms
+ The idle timeout for a connection before it is closed.
+ default: 600000

#### opscfg.hera.server.transaction_idle_timeout_ms
+ The idle timeout for a connection which has started but not completed a transaction.
+ default: 900000

#### opscfg.hera.server.max_lifespan_per_child
+ The approximate time in seconds after which a worker is re-started. If it is 0 then the worker is never re-started.
+ default: 0

#### opscfg.hera.server.max_requests_per_child
+ The number of requests the worker will process before it is re-started. If it is 0 then the worker is never re-started.
+ default: 0

#### opscfg.hera.server.saturation_recover_threshold
+ The threshold, in ms, for saturation recovering. The SQL eviction is invoked when backlog is higher than <<backlog_pct>> and SQL is found to be running higher than saturation_recover_threshold
+ default: 200

#### opscfg.hera.server.saturation_recover_throttle_rate
+ The throtle rate for the saturation recovery
+ default: 0



 
