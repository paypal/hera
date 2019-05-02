# worker-go
worker connecting to mysql implemented in golang

version 0.1
===========

notes:
---------
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
