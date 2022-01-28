# YCQL Sample


## Build

```bash

git clone <repo> sample
cd sample
./mvnw install

```

## Run

**Data Generator**

```bash
java -cp "target/ycql-hello-world-1.0.jar:target/lib/*" com.yugabyte.sample.apps.
DataGenerator

```


**Query Runner**

```bash
java -cp "target/ycql-hello-world-1.0.jar:target/lib/*" com.yugabyte.sample.apps.
Sample

```



## Modify

Update [src/main/java/com/yugabyte/sample/apps/DataGenerator.java](src/main/java/com/yugabyte/sample/apps/DataGenerator.java) to change the number off Records created


```java
private static final int VENDOR_COUNT = 25;
private static final int DOMAIN_COUNT = 25;
private static final int TECH_COUNT = 5;
private static final int RECORD_PER_COMBO = 100;
```

Update above constants to change record counts


Open [src/main/java/com/yugabyte/sample/apps/Sample.java](src/main/java/com/yugabyte/sample/apps/Sample.java) to change the query

```java
private static final int THREADPOOL_SIZE=20;
private static final int QUERY_VENDOR_START = 1;
private static final int QUERY_VENDOR_END = 24;
private static final int QUERY_DOMAIN_START = 1;
private static final int QUERY_DOMAIN_END = 15;

```
Update above contants to change the query params
