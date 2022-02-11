package com.yugabyte.sample.apps;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.slf4j.Logger;


public class Sample {

  private static final Logger logger = getLogger(Sample.class);
  private static final int THREADPOOL_SIZE = 20;
  private static final int QUERY_VENDOR_START = 1;
  private static final int QUERY_VENDOR_END = 24;
  private static final int QUERY_DOMAIN_START = 1;
  private static final int QUERY_DOMAIN_END = 15;


  private static final String query = "select count(*) from test.sensor_data where "
    + "vendor = ? and domain = ? and creation_date = ?;";

  private ExecutorService executor;
  private Cluster cluster;

  public Sample() {
    this.executor = newFixedThreadPool(THREADPOOL_SIZE);
    this.cluster = Cluster.builder()
      .addContactPoint("")
      .withCredentials("yugabyte", "")
      .build();
    this.cluster.getConfiguration()
      .getCodecRegistry()
      .register(LocalDateCodec.instance);
  }

  public long getCount(List<String> vendors, List<String> domains, LocalDate date) {
    logger.info("Processing V:[{}], D:[{}], Dt:[{}] ", vendors, domains,
      date);

    long count = vendors.stream()
      .map(vendor -> {
        return domains.stream()
          .map(domain -> {
            logger.trace("Scheduling V:[{}], D:[{}], Dt:[{}] ", vendor, domain, date);
            return executor.submit(() -> {
              return executeQuery(vendor, domain, date);
            });
          })
          .collect(toList());
      })
      .flatMap(Collection::stream)
      .mapToLong(value -> {
        try {
          return value.get();
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          throw new RuntimeException(e);
        }
      })
      .sum();
    return count;
  }

  private Long executeQuery(String vendor, String domain, LocalDate date) {

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


  public static void main(String[] args) {

    var vendors = range(QUERY_VENDOR_START, QUERY_VENDOR_END + 1).mapToObj(i -> String.format(
        "VENDOR-%1$s", i))
      .collect(toList());
    var domains = range(QUERY_DOMAIN_START, QUERY_DOMAIN_END + 1).mapToObj(i -> String.format(
        "DOMAIN-%1$s", i))
      .collect(toList());
    var date = LocalDate.now();
    Sample sample = new Sample();
    var count = sample.getCount(vendors, domains, date);

    logger.info("Vendors: {}", vendors.size());
    logger.info("Domains: {}", domains.size());
    logger.info("Date: {}", date);
    logger.info("Count: {}", count);
    System.exit(0);

  }

}
