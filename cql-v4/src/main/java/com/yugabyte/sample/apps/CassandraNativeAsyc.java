package com.yugabyte.sample.apps;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

public class CassandraNativeAsyc {

  private static final Logger logger = getLogger(CassandraNativeAsyc.class);


  private static final String query = "select count(*) from test.sensor_data where "
    + "vendor = ? and domain = ? and creation_date = ?;";

  private CqlSessionBuilder sessionBuilder;

  public CassandraNativeAsyc(CqlSessionBuilder sessionBuilder) {
    this.sessionBuilder = sessionBuilder;
  }


  public long getCount(List<String> vendors, List<String> domains, LocalDate date) {
    try(CqlSession session = sessionBuilder.build()) {
      PreparedStatement selectStatement = session.prepare(query);
      var count = new AtomicLong();

      for (var vendor : vendors) {
        for (var domain : domains) {
          session.executeAsync(selectStatement.bind(vendor, domain, date))
            .whenComplete((ars, ex) -> {
              long l = ars.one().get(0, Long.class);
              count.addAndGet(l);
            });
        }
      }
      System.out.println(
        String.format(
          "LimitConcurrencyRequestThrottler finished executing %s queries with a concurrency level of %s.",
          vendors.size() * domains.size(),
          session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS)));
      return count.get();
    }catch(Exception ex){
      logger.error("Execution failed");
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) {
    var QUERY_VENDOR_START = 1;
    var QUERY_VENDOR_END = 24;
    var QUERY_DOMAIN_START = 1;
    var QUERY_DOMAIN_END = 15;

    var vendors = range(QUERY_VENDOR_START, QUERY_VENDOR_END + 1).mapToObj(i -> String.format(
        "VENDOR-%1$s", i))
      .collect(toList());
    var domains = range(QUERY_DOMAIN_START, QUERY_DOMAIN_END + 1).mapToObj(i -> String.format(
        "DOMAIN-%1$s", i))
      .collect(toList());
    var date = LocalDate.now();
    CassandraNativeAsyc sample = new CassandraNativeAsyc(CqlSession.builder());
    var count = sample.getCount(vendors, domains, date);

    logger.info("Vendors: {}", vendors.size());
    logger.info("Domains: {}", domains.size());
    logger.info("Date: {}", date);
    logger.info("Count: {}", count);
    System.exit(0);

  }

}
