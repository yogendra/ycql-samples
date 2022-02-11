package com.yugabyte.sample.apps;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;


/**
 * This example simulates
 */
public class CassandraNativeAsync {

  private static final Logger logger = getLogger(CassandraNativeAsync.class);


  private static final String query = "select count(*) from test.sensor_data where "
    + "vendor = ? and domain = ? and creation_date = ?;";

  private final Cluster cluster;
  public CassandraNativeAsync(Cluster cluster) {
    this.cluster = cluster;
  }



  public long getCount(List<String> vendors, List<String> domains, LocalDate date) {

    logger.info("Processing V:[{}], D:[{}], Dt:[{}] ", vendors, domains, date);
    var session = cluster.connect();
    var selectStatement = session.prepare(query);

    List<ResultSetFuture> futureResultList = new ArrayList<>();
    for (var vendor : vendors) {
      for (var domain : domains) {
        var boundStatement = selectStatement.bind(vendor, domain, date);
        var futureResult = session.executeAsync(boundStatement);
        futureResultList.add(futureResult);
      }
    }

    var count = 0L;
    for(ResultSetFuture futureResult : futureResultList){
        ResultSet rs = futureResult.getUninterruptibly();
        count += rs.one().get(0,Long.class);
    }

    var localConcurrency  =
      session.getCluster().getConfiguration().getPoolingOptions().getMaxRequestsPerConnection(
      HostDistance.LOCAL);
    var remoteConcurrency =
      session.getCluster().getConfiguration().getPoolingOptions().getMaxRequestsPerConnection(HostDistance.REMOTE);

    logger.info("Finished executing {} queries with a concurrency levels: Local: {} and Remote: "
      + "{}",futureResultList.size(), localConcurrency, remoteConcurrency);

    return count;
  }

  public static void main(String[] args) {
    var QUERY_VENDOR_START = 1;
    var QUERY_VENDOR_END = 24;
    var QUERY_DOMAIN_START = 1;
    var QUERY_DOMAIN_END = 15;



    var cluster = Cluster.builder()
      .addContactPoint("")
      .withCredentials("yugabyte", "")
      .build();

    cluster.getConfiguration()
      .getCodecRegistry()
      .register(LocalDateCodec.instance);

    CassandraNativeAsync sample = new CassandraNativeAsync(cluster);

    var vendors = range(QUERY_VENDOR_START, QUERY_VENDOR_END + 1).mapToObj(i -> String.format(
        "VENDOR-%1$s", i))
      .collect(toList());
    var domains = range(QUERY_DOMAIN_START, QUERY_DOMAIN_END + 1).mapToObj(i -> String.format(
        "DOMAIN-%1$s", i))
      .collect(toList());

    var date = LocalDate.now();

    var count = sample.getCount(vendors, domains, date);

    logger.info("Vendors: {}", vendors.size());
    logger.info("Domains: {}", domains.size());
    logger.info("Date: {}", date);
    logger.info("Count: {}", count);
    System.exit(0);

  }

}
