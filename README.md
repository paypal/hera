[![Build Status](https://travis-ci.org/paypal/hera.svg?branch=master)](https://travis-ci.org/paypal/hera)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
# Hera - High Efficiency Reliable Access to data stores

Hera multiplexes connections for MySQL and
Oracle databases.  It supports sharding the databases for horizontal scaling.

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

For development, the following docker commands can help get started

    docker run --network host --name mysql-11 -e MYSQL_ROOT_PASSWORD=62-AntHill -d mysql:latest
    docker exec -it mysql-11 bash -c 'echo "create database testschema;" | mysql -u root -h 127.0.0.1 -p62-AntHill'
    cd hera/tests/devdocker
    mkdir -p src/github.com/paypal/hera
    rsync -av --exclude tests/devdocker ../.. src/github.com/paypal/hera
    docker build -t hera-oss .
    docker run -it --rm --name testRunHeraOss --network host -e password=62-AntHill hera-oss

To test it, in a separate terminal:

    docker exec -it testRunHeraOss /bin/bash
    cd /go/src
    go run github.com/paypal/hera/client/gosqldriver/tls/example/sample_main.go

## Manual Build

The following sections explain the process for manually building mux without Docker. We only tested on RedHat and Ubuntu.

### Install Dependencies

1.  [Install Go 1.10+](http://golang.org/doc/install).
2.  Install [MySQL](http://dev.mysql.com/downloads/mysql) or [Oracle](https://www.oracle.com/index.html).
3.  Install the [MySQL driver](https://github.com/go-sql-driver/mysql) and the [Oracle driver](https://github.com/go-goracle/goracle)
3.  Install Oracle instant client     

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
3.  Install the dependencies: MySQL and Oracle driver
    ```
    go get gopkg.in/goracle.v2
    go get github.com/go-sql-driver/mysql
    ```
4.  Build server binaries
    ```
    go install github.com/paypal/hera/mux github.com/paypal/hera/worker/mysqlworker github.com/paypal/hera/worker/oracleworker
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

    # start
    ./mux --name hera-test
```    
For details about the parameters see [configuration](docs/configuration.md)

## Running the client

There is a Java client implemented as JDBC driver. Please see the [documentation](https://github.com/paypal/hera/tree/master/client/java) for how to install and use it.

There is also a Go client implemented as [SQL driver](client/gosqldriver). Please see the [example](tests/e2e/client).

## License

Hera is licensed under Apache 2.0.
