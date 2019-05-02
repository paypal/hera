# jmux - A Java Client for mux
[![Build Status](https://img.shields.io/travis/com/paypal/jmux.svg?style=flat-square)](https://travis-ci.com/paypal/jmux)
[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://img.shields.io/maven-central/v/io.mux/jmux-core.svg?style=flat-square)](https://search.maven.org/#search%7Cga%7C1%7Cio.mux)
[![GitHub release](https://img.shields.io/github/release/paypal/jmux.svg?style=flat-square)](https://github.com/paypal/jmux/releases)
[![Javadocs](http://www.javadoc.io/badge/io/mux/jmux-core.svg)](http://www.javadoc.io/doc/io/mux/jmux-core)

jmux is the JDBC driver for [mux](https://github.com/paypal/mux).

## Java Versions

Java 8 or above is required.

## Download

### Maven
```xml
<dependency>
  <groupId>jmux-parent</groupId>
  <artifactId>jmux-jdbc</artifactId>
  <version>${jmux-version}</version>
</dependency>
```

Development snapshots are available in [Sonatypes's snapshot repository](https://oss.sonatype.org/content/repositories/snapshots/paypal/mux).

### Usage

```java
String host = "1:127.0.0.1:10101"; 
Properties props = new Properties();
// Override any default property
props.setProperty(OCCClientConfigHolder.RESPONSE_TIMEOUT_MS_PROPERTY, "3000");
// register
Class.forName("com.paypal.jmux.jdbc.OccDriver");
dbConn = DriverManager.getConnection("jdbc:occ:" + host, props);

// do standard JDBC
PreparedStatement pst = dbConn.prepareStatement("select 'abc' from dual");
ResultSet rs = pst.executeQuery();
if (rs.next()) {
	rs.getString(1);
}
```

### Example

The [examples](examples) folder has standalone projects that show usage of jmux.

### Driver properties

For the list of connection properties that can be used with DriverManager.getConnection() see [properties](doc/properties.md)

## Versioning

The project follows [Semantic Versioning](http://semver.org/).

The current major version is zero (0.y.z). Anything may change at any time. The public API should not be considered stable.

## Running tests

The project is tested against a `mux` setup

```sh
$ mvn test -DSERVER_URL=1:<serverNameOrIP>:<port> -Dorg.slf4j.simpleLogger.defaultLogLevel=info
...

-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running com.paypal.jmux.client.NetstringWriterTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.084 sec
Running com.paypal.jmux.jdbc.BatchTest
Tests run: 6, Failures: 0, Errors: 0, Skipped: 6, Time elapsed: 0 sec
Running com.paypal.jmux.jdbc.ClientTest
08:15:50.406 [main] INFO com.paypal.jmux.client.OccClientImpl - Setup OK
38-10
ping took (ms):182
08:17:06.061 [main] INFO com.paypal.jmux.client.OccClientImpl - Done
Tests run: 43, Failures: 0, Errors: 0, Skipped: 7, Time elapsed: 78.12 sec
Running com.paypal.jmux.jdbc.ShardingTest
Shard #: 1
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 7.16 sec
Running com.paypal.jmux.jdbc.TestTest
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

## Contact

TODO

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## Reporting bugs

See [reporting bugs](doc/reporting_bugs.md) for details about reporting any issues.

## License

Unless otherwise noted the jmux source files are distributed under the Apache 2.0 license. See the [LICENSE](LICENSE.txt) file for details.
