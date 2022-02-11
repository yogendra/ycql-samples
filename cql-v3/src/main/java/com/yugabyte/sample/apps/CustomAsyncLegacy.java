package com.yugabyte.sample.apps;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;

public class CustomAsyncLegacy {

  private static final Logger logger = getLogger(CustomAsyncLegacy.class);
  private static final int THREADPOOL_SIZE = 20;


  private static final String query = "select count(*) from test.sensor_data where "
    + "vendor = ? and domain = ? and creation_date = ?;";

  private ExecutorService executor;
  private Cluster cluster;

  public CustomAsyncLegacy(Cluster cluster) {
    this.executor = newFixedThreadPool(THREADPOOL_SIZE);
    this.cluster = cluster;
  }


  public long getCount(List<String> vendors, List<String> domains, LocalDate date) {
    logger.info("Processing V:[{}], D:[{}], Dt:[{}] ", vendors, domains, date);
    List<Future<Long>> jobs = new ArrayList<>(vendors.size() * domains.size());
    for (var vendor : vendors) {
      for (var domain : domains) {
        var job = new QueryExecutionJob(vendor, domain, date, cluster);
        var future = executor.submit(job);
        jobs.add(future);
      }
    }
    long count = 0;
    for(Future<Long> job: jobs){
      try {
        count += job.get();
      } catch (Exception e) {
        logger.error("Failed to get count", e);
        throw new RuntimeException(e);
      }
    }
    return count;
  }

  public static class QueryExecutionJob implements Callable<Long> {
    private static final Logger logger = getLogger(QueryExecutionJob.class);
    private final String vendor;
    private final String domain;
    private final LocalDate date;
    private final Cluster cluster;

    public QueryExecutionJob(String vendor, String domain, LocalDate date, Cluster cluster) {
      this.vendor = vendor;
      this.domain = domain;
      this.date = date;
      this.cluster = cluster;
    }


    @Override
    public Long call() throws Exception {
      try (Session session = cluster.newSession ()) {
        var rs = session.execute(query, vendor, domain, date);
        var count = rs.one()
          .get(0, Long.class);
        session.close();
        return count;
      } catch (Exception e) {
        logger.error("Failed to execute CQL", e);
        throw new RuntimeException(e);
      }
    }
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

    CustomAsyncLegacy sample = new CustomAsyncLegacy(cluster);


    var vendors = new ArrayList<String>(QUERY_VENDOR_END);
    var domains = new ArrayList<String>(QUERY_DOMAIN_END);

    for(var i: range(QUERY_VENDOR_START, QUERY_VENDOR_END+1).toArray()){
      vendors.add(String.format("VENDOR-%1$s", i));
    }
    for(var i: range(QUERY_DOMAIN_START, QUERY_DOMAIN_END+1).toArray()){
      domains.add(String.format("DOMAIN-%1$s", i));
    }
    var date = LocalDate.now();

    var count = sample.getCount(vendors, domains, date);

    logger.info("Vendors: {}", vendors.size());
    logger.info("Domains: {}", domains.size());
    logger.info("Date: {}", date);
    logger.info("Count: {}", count);
    System.exit(0);

  }

}
