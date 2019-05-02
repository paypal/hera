To run the tests these environment variables need to be set
1. LD_LIBRARY_PATH to include the OCI folder
2. TWO_TASK - the database URL, like (DESCRIPTION=(ADDRESS_LIST=(FAILOVER = ON)(LOAD_BALANCE = ON)(ADDRESS=(PROTOCOL=TCP)(Host=somemachine.com)(PORT=1234)))(CONNECT_DATA=(SERVICE_NAME=someservice)))
4. username - the database username
5. password - the databse password
6. sqlpluscmd - the path to sqlplus, used for setup
7. TABLE_NAME (optional) - the table name used by tests, default jdbc_mux_test
8. MGMT_TABLE_PREFIX (optional) - the prefix used for the management tables, default empty string

One time, run ./setup.sh to create the necessary tables and tables data
