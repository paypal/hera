# hera-jdbc - A Java Client for Hera
[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/com.paypal.hera/hera-jdbc.svg?style=flat-square)](https://search.maven.org/#search%7Cga%7C1%7Ccom.paypal.hera)

hera-jdbc is the JDBC driver for [hera](https://github.com/paypal/hera).

## Java Versions

Java 8 or above is required.

## Download

### Maven

First build the Hera JDBC driver:
```sh
mvn install -f client/java/jdbc/pom.xml -Dmaven.test.skip=true
```
Add this to your pom:
```xml
<dependency>
  <groupId>com.paypal</groupId>
  <artifactId>hera-jdbc</artifactId>
  <version>${hera-jdbc-version}</version>
</dependency>
```
### Usage

```java
String host = "1:127.0.0.1:11111"; 
Properties props = new Properties();
// Override any default property
props.setProperty(HeraClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
dbConn = DriverManager.getConnection("jdbc:hera:" + host, props);

// do standard JDBC
PreparedStatement pst = dbConn.prepareStatement("select 'abc' from dual");
ResultSet rs = pst.executeQuery();
if (rs.next()) {
	rs.getString(1);
}
```

### Example

The [examples](examples) folder has standalone projects that show usage of hera-jdbc.

### Driver properties

For the list of connection properties that can be used with DriverManager.getConnection() see [properties](doc/properties.md)

## Versioning

The project follows [Semantic Versioning](http://semver.org/).

The current major version is zero (0.y.z). Anything may change at any time. The public API should not be considered stable.

## Running tests

The project is tested against a `hera` setup

```sh
$ mvn test -DSERVER_URL=1:<serverNameOrIP>:<port> -Dorg.slf4j.simpleLogger.defaultLogLevel=info
...

-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running com.paypal.hera.client.NetstringWriterTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.084 sec
Running com.paypal.hera.jdbc.BatchTest
Tests run: 6, Failures: 0, Errors: 0, Skipped: 6, Time elapsed: 0 sec
Running com.paypal.hera.jdbc.ClientTest
08:15:50.406 [main] INFO com.paypal.hera.client.HeraClientImpl - Setup OK
38-10
ping took (ms):182
08:17:06.061 [main] INFO com.paypal.hera.client.HeraClientImpl - Done
Tests run: 43, Failures: 0, Errors: 0, Skipped: 7, Time elapsed: 78.12 sec
Running com.paypal.hera.jdbc.ShardingTest
Shard #: 1
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.16 sec
Running com.paypal.hera.jdbc.TestTest
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.825 sec

Results :

Tests run: 57, Failures: 0, Errors: 0, Skipped: 13

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  01:32 min
[INFO] Finished at: 2019-01-15T08:17:16-08:00
[INFO] ------------------------------------------------------------------------
````


