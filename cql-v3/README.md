# YCQL Sample

Collection of sample apps for YCQL

1. [Async Execution and Aggregation](#sample-App--async-execution-and-ggregation)

## Build

```bash

git clone <repo> samples
cd samples/cql-v3
./mvnw clean install
```


## Sample App: Async Execution and Aggregation

This application shows how to execute queries asynchronously and aggregate results. There are
three variants of the app.
1. CassandraNativeAsync: Uses native cassandra async function.
2. CustomAsync: Uses customer executor to execute queries and aggregate results
3. CustomAsyncLegacy: Just a syntactical variant on #2, using legacy loops for readability

### Run

**Data Generator**

```bash
java -cp "target/cql-v3-1.0.jar:target/lib/*" com.yugabyte.sample.apps.DataGenerator
```

### Run Sample to Query Data

**Option 1: Query via Cassandra Native Async**

```bash
java -cp "target/cql-v3-1.0.jar:target/lib/*" com.yugabyte.sample.apps.CassandraNativeAsync

```

**Option 2: Query via Custom Async**

```bash
java -cp "target/cql-v3-1.0.jar:target/lib/*" com.yugabyte.sample.apps.CustomAsync

```

**Option 3: Query via Custom Async with Legacy Loops**

```bash
java -cp "target/cql-v3-1.0.jar:target/lib/*" com.yugabyte.sample.apps.CustomAsyncLegacy

```


**Expected Output**

```
42 [main] INFO com.datastax.driver.core - DataStax Java driver 3.10.3-yb-2 for Apache Cassandra
53 [main] INFO com.datastax.driver.core.GuavaCompatibility - Detected Guava >= 19 in the classpath, using modern compatibility layer
386 [main] INFO com.datastax.driver.core.ClockFactory - Using native clock to generate timestamps.
500 [main] INFO com.yugabyte.sample.apps.CassandraNativeAsync - Processing V:[[VENDOR-1, VENDOR-2, VENDOR-3, VENDOR-4, VENDOR-5, VENDOR-6, VENDOR-7, VENDOR-8, VENDOR-9, VENDOR-10, VENDOR-11, VENDOR-12, VENDOR-13, VENDOR-14, VENDOR-15, VENDOR-16, VENDOR-17, VENDOR-18, VENDOR-19, VENDOR-20, VENDOR-21, VENDOR-22, VENDOR-23, VENDOR-24]], D:[[DOMAIN-1, DOMAIN-2, DOMAIN-3, DOMAIN-4, DOMAIN-5, DOMAIN-6, DOMAIN-7, DOMAIN-8, DOMAIN-9, DOMAIN-10, DOMAIN-11, DOMAIN-12, DOMAIN-13, DOMAIN-14, DOMAIN-15]], Dt:[2022-02-11]
561 [main] INFO com.datastax.driver.core.NettyUtil - Did not find Netty's native epoll transport in the classpath, defaulting to NIO.
1103 [main] INFO com.datastax.driver.core.policies.DCAwareRoundRobinPolicy - Using data-center name 'datacenter1' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)
1105 [main] INFO com.datastax.driver.core.Cluster - New Cassandra host localhost/127.0.0.1:9042 added
1105 [main] INFO com.datastax.driver.core.Cluster - New Cassandra host /127.0.0.3:9042 added
1106 [main] INFO com.datastax.driver.core.Cluster - New Cassandra host /127.0.0.2:9042 added
1685 [main] INFO com.yugabyte.sample.apps.CassandraNativeAsync - Vendors: 24
1685 [main] INFO com.yugabyte.sample.apps.CassandraNativeAsync - Domains: 15
1687 [main] INFO com.yugabyte.sample.apps.CassandraNativeAsync - Date: 2022-02-11
1687 [main] INFO com.yugabyte.sample.apps.CassandraNativeAsync - Count: 439000

```

**Change Query params**

Update [DataGenerator.java] to change the number off Records created


```java
class DataGenerator{
  // ...
  private static final int VENDOR_COUNT = 25;
  private static final int DOMAIN_COUNT = 25;
  private static final int TECH_COUNT = 5;
  private static final int RECORD_PER_COMBO = 100;
  /...
}
```

Update above constants to change record counts



Open Java source for the query clas ([CassandraNativeAsync.java], [CustomAsyncLegacy.java],
or [CustomAsync.java])
to change the query

```java
private static final int THREADPOOL_SIZE=20;
private static final int QUERY_VENDOR_START = 1;
private static final int QUERY_VENDOR_END = 24;
private static final int QUERY_DOMAIN_START = 1;
private static final int QUERY_DOMAIN_END = 15;

```
Update above constants to change the query params


[DataGenerator.java]: src/main/java/com/yugabyte/sample/apps/DataGenerator.java
[CassandraNativeAsync.java]: src/main/java/com/yugabyte/sample/apps/CassandraNativeAsync.java
[CustomAsync.java]: src/main/java/com/yugabyte/sample/apps/CustomAsync.java
[CustomAsyncLegacy.java]: src/main/java/com/yugabyte/sample/apps/CustomAsyncLegacy.java
