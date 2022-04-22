1. mysqlworker - written in go
2. cppworker - for Oracle in C++ (preferred over oracleworker)
3. oracleworker - written in go (lacks sql rewrite for sharding, oci break..)
4. postgresworker - written in go

mysqlworker
-----------
1. dependencies: 
   - go get -u github.com/go-sql-driver/mysql

2. setup mysql:
   - download mysql-5.7.18-linux-glibc2.5-x86_64.tar.gz from https://www.mysql.com/downloads/
      - wget --no-check-certificate https://downloads.mysql.com/archives/get/file/mysql-5.7.18-linux-glibc2.5-x86_64.tar.gz
      - it is under "MySQL Community Edition"/"MySQL Community Server (GPL)"/"Archived versions"/"MySQL Community Server". choose "5.7.18", "Linux - Generic", "Linux - Generic (glibc 2.5)(x86, 64-bit)"
   - tar -xzvf mysql-5.7.18-linux-glibc2.5-x86_64.tar.gz
   - following instructions under "https://dev.mysql.com/doc/refman/5.7/en/binary-installation.html"
   - make sure to copy down the temporary password. e.g. "2017-11-17T21:04:01.772389Z 1 [Note] A temporary password is generated for root@localhost: E1u2OJHd6dxZ"
   - set your PATH to include mysql/bin
   - mysql -u root -p
   - change root password: ALTER USER 'root'@'localhost' IDENTIFIED BY 'account';
   - create new users.
      - mysql> create user 'someuser'@'localhost' identified by 'abc123';
      - mysql> grant all privileges on *.* to 'someuser'@'localhost' with grant option;
   - In golang, use something similar to sql.Open("mysql", "someuser:abc123@tcp(10.3.3.3:3306)/mysql")
   - mysql talbe names are case sensitive on linux. create table in lower case and set
```
[mysqld]
lower_case_table_names = 1

in /etc/my.cnf
```


```
// debugging some security and connectivity between db and hera
//mismatch ca
14:00:19.886927 warn: [WORKER 1 619757 adapter.go:129] could not get connection x509: certificate signed by unknown authority

//mismatch SAN/CN
13:41:00.034136 warn: [WORKER 4 616871 adapter.go:129] could not get connection x509: certificate is valid for mysql.server.example.com, not mismatch.mysqldb.example.com

//mismatch cert file
13:33:08.643969 warn: [WORKER 3 615657 adapter.go:129] could not get connection x509: cannot validate certificate for 10.11.22.33 because it doesn't contain any IP SANs

//missing cert file
13:17:02.958192 warn: [WORKER 0 2032 adapter.go:97] recycling, got read-only conn retry-attempt=1
13:17:02.958277 warn: [WORKER 0 2032 adapter.go:97] recycling, got read-only conn retry-attempt=2
13:17:02.958443 warn: [WORKER 0 2032 adapter.go:97] recycling, got read-only conn retry-attempt=3
13:17:02.958872 warn: [WORKER 0 2032 cmdprocessor.go:812] driver error cannot use read-only conn tcp(10.11.22.33:3306)/worldreadydb?timeout=9s&clientFou
13:17:02.959123 warn: [WORKER 0 2032 workerservice.go:130] Can't connect to DB: cannot use read-only conn tcp(10.11.22.33:3306)/binkywinkydb?timeout=9s&
```
