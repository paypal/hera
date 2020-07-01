# Bind Eviction and Throttle

During overloads, SQL eviction find long running queries and removes them.
This is similar to what DBAs do to recover from an overload.  We would
extend the eviction and use the same trigger.  Bind eviction will analyze
running queries' binds like a DBA would do manually.  If the set of
currently running sessions has many instances of a particular query
or bind name-value, the DBAs would block them with triggers.  The hera
 code it's own internal session information structure to evict and
temporarily block excessive queries or bind name-values.

*The automatic bind value blocking would trigger when the hera is
overloaded and the backlog of queries hasn't cleared in 1s.  If 25%
(default `bind_eviction_threshold_pct=25`) of the connections use a
particular bind value, then some of those
bind-value queries should be evicted.*

We require that the bind value be at least 8 bytes. This avoids blocking 
status == 'D' or zip_code == '00901'. Each sql text is blocked separately.

# Dynamic Throttle

A simple 20s time block would work against a troublesome batch job that retries
immediately on failures.  But when the block expires, the DB would
be flooded again and cause errors for all users.

To keep the flood visible on db-hera connection monitors, the throttle 
allows more bind-queries when the connection usage is lower.

We want to shield the DB from having
too many queries—even if they get timed out.  If the application
sends a query while we have high connections in use, we’d block more
queries—allow-1-out-of-2 goes up to allow-1-out-of-5.  If the queries
are under the connections use limit, we allow more in—allow-1-out-of-44
goes down to allow 1-out-of-43.

## Sufficient blocking to keep DB safe

A busy pool might have 500m queries/hr across 150 nodes.  That’s
about 3.3m/hr per node.   It’ll be about 1k/s on each node.
Allow-1-out-of-10,000 would send 1 query every 10s to the db. 

Even if a query+bind retries immediately on any failure, they’d be
pushing 150,000 queries/s--perhaps 1k api/s. And they’d be seeing 99.99%
failures or more.

Based on allow-1-out-of-10,000 block, the db would be getting 15
queries/s.

## Automatic restoration

We have a time-based reset for allow-1-out-of-X ---- capping X at
10,000 and decrementing X every second.
If the query+bind stops for 4hrs, service is automatically restored. 

If the query+bind throttles down, so each of the queries that makes
it to the DB doesn’t timeout—select-for-update has no contention.
Then, 10,000 successful queries restores service—just under 50 million
query attempts needed—5.5min at 150k/s.

If the dynamic throttle isn't needed, it could be disabled by configuring
`bind_eviction_decr_per_sec = 10000.0` (default 1.0) to remove the throttle in the next
second.

## DBA Adjustment Levers

If a query+bind is blocked, restarting hera will clear out the block.
If the traffic ramps up high again, the block could be triggered.

To clear monitoring, we could
reconfigure the target connection use percentage from 50% (default) to 30% 
(`bind_eviction_target_conn_pct = 30`) and restart hera.  

We don't expect the adjustment levers to be needed since the trigger
point is hera bouncing connections and evicting sql.

# Monitoring Details

When Bind Eviction happens, CalEvents are logged for each connection that is 
evicted.  The sqlhash and bind key-value can help trace.

    E17:04:39.62    BIND_EVICT      2312453186      1       pid=5913&k=p1&v=29001111
    E17:04:39.62    BIND_EVICT      2312453186      1       pid=6071&k=p1&v=29001111


During throttling, some queries are blocked and hera server logs CalEvents and 
sends the client the `HERA-105: bind throttle` error. Below is a message that
blocked :p1 with value 29001111.

    E17:04:39.67    BIND_THROTTLE   2312453186      1       k=p1&v=29001111&allowEveryX=67&allowFrac=0.01493&raddr=127.0.0.1:37676

The hera application log
has incr and decr lines with allow-every-x details. These are logged when a query
is allowed to go to the DB or if there hasn't been a query+bind for 1s or more.

    17:04:39.668449 warn: [PROXY bindevict.go:89] bind throttle incr hash:2312453186 bindName:p1 val:29001111 prev:25
    17:04:34.973123 warn: [PROXY bindevict.go:72] bind throttle decr hash:2312453186 bindName:p1 val:29001111 allowEveryX:2065-2

