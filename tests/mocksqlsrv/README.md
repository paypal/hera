#
Basic MySQL server implementation

Follows MySQL protocol and offers very basic capabilities. Not connected to a storage
engine and currently does not properly return result sets in response to client com_query
packets. Does not implement user authentication, SQL syntax parser, or SSL. Will pass tests
that only check that results are returned when requested——will NOT pass tests checking for
accurate query results.


## Getting started

First start up the server in the terminal. Then you can run tests against the
server in a separate terminal window.

## Running the server from terminal

Users have the option of entering a port number and a float between 0 and 1.

The server will run on the specified port on local. The float represents the probability
that the server fails and sends a random error message to the client during
the command phase.

For example, we can run the mocksqlsrv on localhost:3333 with a 20% chance of
failure.

```
chmod +x setup

./setup 3333 0.2
```

To stop the server, Ctrl-C or just exit the terminal window.

## Running tests against the server

Depending on the test you want to run against the server, there may be
a setup shell script already provided so that symlinking / directory
navigation / environment variable setting is completed for you.

### 1. samplequeries.go

`samplequeries.go` contains a series of queries and prepared statements that are
executed against the server. You can run it in a different terminal window
from the server by typing `go run samplequeries.go`.

### 2. multiconn

This script runs ten different samplequeries.go instances at the same time, so
you can see how the server handles multiple connections at once. To execute,
modify the permissions (if necessary) and run in the terminal like specified
above.

### 3. run_unit

The default configuration in `coordinator_basic` does not assume MySQL, as well
as other tests in `unittest`.

* In `hera/tests/unittest/[test_name]/main_test.go`, make sure that in `cfg()`,
you have the line `appcfg[child.executable] = "mysqlworker"`.
* If `cfg()` returns a `testutil.WorkerType` `testutil.OracleWorker`, change it to `testutil.MySQLWorker`.
* This server probably only works with `coordinator_basic` though.

`run_unit` sets up environment variables. This script automatically executes coordinator_basic.
To change this, replace `n=coordinator_basic` with `n=desired_test_name`. This will run the specified unittest.

To execute, modify the permissions (if necessary) of run_unit and run in the terminal like
specified below.
```
chmod +x run_unit
./run_unit
```

* NOTE: For low or high probability of failure, behavior is generally consistent
in that the test runs and then exits by passing or failing. However, at `p=0.5`,
the coordinator_basic test will sometimes segfault.
