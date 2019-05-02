# Getting Started

You can build mux using either [Docker](#docker-build) or [manual](#manual-build) build.

## Docker Build

For development, the following docker commands can help get started
    docker run --network host --name mysql-11 -e MYSQL_ROOT_PASSWORD=62-AntHill -d mysql:latest

    cd src/go.mux/mux/tests/devdocker
    cp -rl `d=../../ ; ls $d | sed -e "s,^,$d," | grep -v /tests$` mux

    cd ../../..
    docker build -t occ-oss mux/tests/devdocker/ 
    docker run -it --rm --name testRunOccOss --network host -e password=62-AntHill occ-oss &
    docker exec -it testRunOccOss /bin/bash
    cd /go/src
    go run go.mux/mux/gomuxdriver/muxtls/example/sample_main.go
## Manual Build

The following sections explain the process for manually building mux without Docker. We only tested on RedHat and Ubuntu.

### Install Dependencies

1.  [Install Go 1.10+](http://golang.org/doc/install).
2.  Install [MySQL](http://dev.mysql.com/downloads/mysql) or Oracle(https://www.oracle.com/index.html).
3.  Install the [MySQL driver](github.com/go-sql-driver/mysql) and the [Oracle driver](https://github.com/go-goracle/goracle)
3.  Install Oracle instant client     
    
### Build Binaries

1.  Navigate to the working directory.
    ```
    cd $WORKSPACE
    export GOPATH=$WORKSPACE
    ```
2.  Clone the source code from [github](https://github.com/paypal/hera)
    ```
    git clone git@github.com:paypal/hera github.com/paypal/hera
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
    go install mux/tests/e2e/client
    ```
6.  Build Java test client under the java directory
    
### Running the server

To run mux there is minimal configuration required. Please see examples for running with [MySQL](https://github.com/paypal/hera/tree/master/tests/e2e/srvmysql) or [Oracle](https://github.com/paypal/hera/tree/master/tests/e2e/srvoracle).
The main configuration file is occ.txt, which must contain the TCP port where the server listens and the number of workers. The user name, password and the data source are passsed via environment parameters.
```    
    # the proxy executable
    ln -s $GOPATH/bin/mux .
    # the MySQL worker
    ln -s $GOPATH/bin/mysqlworker occworker
    # to use the Oracle worker use oracleworker instead of mysqlworker 

    # create the configuration file with the required configuration
    echo 'bind_ip=127.0.0.1' > occ.txt
    echo 'bind_port=11111' >> occ.txt
    echo 'opscfg.occ.server.max_connections=2' >> occ.txt
    
    # create minimal CAL configuration, needed by ops config
    echo 'cal_pool_name=stage_occ' > cal_client.txt
    
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
    ./mux --name main
```    
For details about the parameters see [configuration](configuration.md)

## Running the client

There is a Java client implemented as JDBC driver. Please see the [documentation](https://github.com/paypal/hera/tree/master/java) for how to install and use it.

There is also a Go client implemented as [SQL driver](mux/gomuxdriver). Please see the [example](tests/e2e/client).
