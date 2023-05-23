[![Go Build](https://github.com/paypal/hera/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/paypal/hera/actions/workflows/go.yml)
[![Java CI with Maven](https://github.com/paypal/hera/actions/workflows/jdbc-ci-maven.yml/badge.svg?branch=master)](https://github.com/paypal/hera/actions/workflows/jdbc-ci-maven.yml)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

<img src="docs/hera.png" height="240" width="340">

# Hera - High Efficiency Reliable Access to data stores

Hera multiplexes connections for MySQL, Oracle and
PostgreSQL databases. It supports sharding the databases for horizontal scaling.

  * [Overview](docs/overview.md)
  * [Configuration](docs/configuration.md)
  * [Sharding](docs/sharding.md)
  * [Transparent failover](docs/taf.md)
  * [Bind Eviction](docs/bindevict.md)
  * [History](docs/history.md)
  * [Contributing](docs/contributing.md)

# What is Hera

Hera is Data Access Gateway that helps to enable scaling and improving the availability of databases.
* Protects the database from resource exhaustion by evicting poorly performing queries
* Intelligently routes read/write traffic appropriately for better load balancing
* Improves tolerance to database outages
* Provides high performance secured connections between applications and Hera
* Provides domain agnostic database sharding for horizontal database scaling
* Automatic transaction application failover between replica databases
* And many more site resiliency features

# Getting Started

You can build mux using either [Docker](#docker-build) or [manual](#manual-build) build.

## Docker Build

For development, the following docker commands for the appropriate environment can help get started

Linux

    git clone https://github.com/paypal/hera.git
    docker run --network host --name mysql-11 -e MYSQL_ROOT_PASSWORD=62-AntHill -d mysql:latest
    cd hera/tests/devdocker
    docker exec -i mysql-11 mysql -u root -h 127.0.0.1 -p62-AntHill -t < sample.sql
    mkdir -p src/github.com/paypal/hera
    rsync -av --exclude tests/devdocker ../.. src/github.com/paypal/hera
    docker build -t hera-oss .
    docker run -it --rm --name testRunHeraOss --network host -e password=62-AntHill hera-oss

Mac

    git clone https://github.com/paypal/hera.git
    docker network create my-network
    docker run --network my-network --name mysql-11 -e MYSQL_ROOT_PASSWORD=62-AntHill -d mysql:latest
    cd hera/tests/devdocker
    docker exec -i mysql-11 mysql -u root -h 127.0.0.1 -p62-AntHill -t < sample.sql
    mkdir -p src/github.com/paypal/hera
    rsync -av --exclude tests/devdocker ../.. src/github.com/paypal/hera
    sed -i.bak -e 's/127.0.0.1/mysql-11/g' srv/start.sh
    docker build -t hera-oss .
    docker run -it --rm --name testRunHeraOss --network my-network -p 10101:10101 -e password=62-AntHill hera-oss


To test it, in a separate terminal:

    docker exec -it testRunHeraOss /bin/bash
    cd /go/src
    go run github.com/paypal/hera/client/gosqldriver/tls/example/sample_main.go

## Manual Build

The following sections explain the process for manually building mux without Docker. We only tested on RedHat and Ubuntu.

### Install Dependencies

1.  [Install Go 1.10+](http://golang.org/doc/install).
2.  Install [MySQL](http://dev.mysql.com/downloads/mysql), [Oracle](https://www.oracle.com/index.html) or [PostgresSQL](https://www.postgresql.org/download/).
3.  Install the [MySQL driver](https://github.com/go-sql-driver/mysql), [Oracle driver](https://github.com/go-goracle/goracle) and the [PostgreSQL driver](https://github.com/lib/pq)
4.  Install Oracle instant client.

### Build Binaries

1.  Navigate to the working directory.
    ```
    cd $WORKSPACE
    export GOPATH=$WORKSPACE
    ```
2. Option 1

    Clone the source code from [github](https://github.com/paypal/hera)
    ```
    git clone git@github.com:paypal/hera src/github.com/paypal/hera
    ```
    Option 2

    (a) GO 1.12 is prerequisite

    (b) export GO111MODULE=on ( to enable the go mod feature)
    ```
    go get github.com/paypal/hera
    ```
3.  Install the dependencies: MySQL, Oracle and PostgreSQL driver
    ```
    go get github.com/go-sql-driver/mysql
    go get github.com/godror/godror
    go get github.com/lib/pq
    ```
4.  Build server binaries
    ```
    go install github.com/paypal/hera/mux github.com/paypal/hera/worker/mysqlworker github.com/paypal/hera/worker/oracleworker github.com/paypal/hera/worker/postgresworker
    ```
5.  Build Go test client
    ```
    go install github.com/paypal/hera/tests/e2e/client
    ```
6.  Build Java test client under the client/java directory

7.  Build the C++ oracleworker
    ```
    cd worker/cppworker/worker
    make -f ../build/makefile19
    ```
### Running the server

To run mux there is minimal configuration required. Please see examples for running with [MySQL](https://github.com/paypal/hera/tree/master/tests/e2e/srvmysql) or [Oracle](https://github.com/paypal/hera/tree/master/tests/e2e/srvoracle).
The main configuration file is hera.txt, which must contain the TCP port where the server listens and the number of workers. The user name, password and the data source are passed via environment parameters.
```bash
    # the proxy executable
    ln -s $GOPATH/bin/mux .
    # the MySQL worker
    ln -s $GOPATH/bin/mysqlworker mysqlworker
    # to use the Oracle worker use oracleworker instead of mysqlworker

    # create the configuration file with the required configuration
    echo 'bind_ip=127.0.0.1' > hera.txt
    echo 'bind_port=11111' >> hera.txt
    echo 'opscfg.hera.server.max_connections=2' >> hera.txt
    # if using mysql run this
    # echo 'database_type=mysql' >> hera.txt
    # if using postgres run this
    # echo 'database_type=postgres' >> hera.txt

    # create minimal CAL configuration, needed by ops config
    echo 'cal_pool_name=stage_hera' > cal_client.txt

    # the database user name
    export username='user'
    # the database password
    export password='pass'
    # the data source
    export TWO_TASK='tcp(127.0.0.1:3306)/myschema'
    # for Oracle the datasource can be like '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=hostname)
    #    (PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=sn)))'.
    # for Oracle only add to LD_LIBRARY_PATH environment variable the path to the shared libraries of the
    #    Oracle instant client
    # for postgress format can be like : export TWO_TASK='127.0.0.1:5432/user?connect_timeout=60'

    # start
    ./mux --name hera-test
    
    # to validate HERA is running fine tail 'state-log' file and check 2 connections in 'acpt' state.
    # 01/07/2022 18:28:39: -----------  init  acpt  wait  busy  schd  fnsh  quce  asgn  idle  bklg  strd
    # 01/07/2022 18:28:39: hera            0     2     0     0     0     0     0     0     0     0     0
    
    # incase connections are not in accept state, check the hera.log file for errors
```    
For details about the parameters see [configuration](docs/configuration.md)

## Running the client

There is a Java client implemented as JDBC driver. Please see the [documentation](https://github.com/paypal/hera/tree/master/client/java) for how to install and use it.

There is also a Go client implemented as [SQL driver](client/gosqldriver). Please see the [example](tests/e2e/client).

## License

Hera is licensed under Apache 2.0.
