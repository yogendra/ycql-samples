package com.yugabyte.sample.apps;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;


/**
 * This example simulates
 */
public class CassandraNativeAsyncWithRecord {

  private static final Logger logger = getLogger(
    CassandraNativeAsyncWithRecord.class);


  private static final String query = """
    select * from test.sensor_data
    where vendor = ? and domain = ? and creation_date = ?;
    """;
  private static final int QUERY_VENDOR_START = 1;
  private static final int QUERY_VENDOR_END = 24;
  private static final int QUERY_DOMAIN_START = 1;
  private static final int QUERY_DOMAIN_END = 15;
  private final Cluster cluster;

  public CassandraNativeAsyncWithRecord(Cluster cluster) {
    this.cluster = cluster;
  }


  public List<SensorData> get(List<String> vendors, List<String> domains, LocalDate date) {

    logger.info("Processing V:[{}], D:[{}], Dt:[{}] ", vendors, domains, date);
    var session = cluster.connect();
    var selectStatement = session.prepare(query);
    List<Object[]> argumentList = getArgumentList(vendors, domains);
    var pe = new ParallelExecutor<SensorData>(query, session, argumentList, this::sensorDataMapper);
    List<SensorData> result = pe.execute();
    logSessionInfo(session);
    return result;
  }

  private static List<Object[]> getArgumentList(List<String> vendors, List<String> domains) {
    List<Object[]> argumentList = new ArrayList<>();
    for (var vendor : vendors) {
      for (var domain : domains) {
        var arguments = new Object[]{vendor, domain};
        argumentList.add(arguments);
      }
    }
    return argumentList;
  }


  public static void main(String[] args) {
    Cluster cluster = getCluster();
    CassandraNativeAsyncWithRecord sample = new CassandraNativeAsyncWithRecord(cluster);

    List<String> vendors = getVendorList();
    List<String> domains = getDomainList();

    var date = LocalDate.now();
    var startTime = Instant.now();

    var sensorList = sample.get(vendors, domains, date);
    var endTime = Instant.now();
    logger.info("Vendors: {}", vendors.size());
    logger.info("Domains: {}", domains.size());
    logger.info("Date: {}", date);
    logger.info("Count: {}", sensorList.size());
    logger.info("Start Time: {}", startTime);
    logger.info("End Time: {}", startTime);
    logger.info("Run Time: {}", Duration.between(startTime, endTime));
    System.exit(0);

  }

  private static List<String> getDomainList() {
    var domains = range(QUERY_DOMAIN_START, QUERY_DOMAIN_END + 1).mapToObj(i -> String.format(
        "DOMAIN-%1$s", i))
      .collect(toList());
    return domains;
  }

  private static List<String> getVendorList() {
    var vendors = range(QUERY_VENDOR_START, QUERY_VENDOR_END + 1).mapToObj(i -> String.format(
        "VENDOR-%1$s", i))
      .collect(toList());
    return vendors;
  }

  private static Cluster getCluster() {
    var cluster = Cluster.builder()
      .addContactPoint("")
      .withCredentials("yugabyte", "")
      .build();

    cluster.getConfiguration()
      .getCodecRegistry()
      .register(LocalDateCodec.instance);
    return cluster;
  }

  private static void logSessionInfo(Session session) {
    var localConcurrency =
      session.getCluster()
        .getConfiguration()
        .getPoolingOptions()
        .getMaxRequestsPerConnection(
          HostDistance.LOCAL);
    var remoteConcurrency =
      session.getCluster()
        .getConfiguration()
        .getPoolingOptions()
        .getMaxRequestsPerConnection(HostDistance.REMOTE);
    logger.info("Finished executing queries with a concurrency levels: Local: {} and Remote: {}",
      localConcurrency, remoteConcurrency);
  }


  class SensorData {

    private String domain;
    private String vendor;
    private LocalDate creationDate;
    private String technology;
    private String uniqueIdentifier;
    private LocalDateTime reportingTime;
    private LocalDateTime dumpProcessTime;
    private String senderName;
    private String senderIp;
    private String rca;
    private String rcaId;
    private String rawTrap;
    private String alarmExternalId;
    private String alarmName;
    private String description;
    private String subEntity;
    private LocalDateTime eventTime;
    private String entityId;
    private String entityStatus;
    private String severity;
    private LocalDateTime l1ProcessTime;
    private String rowKey;


  }

  private SensorData sensorDataMapper(Row row) {
    SensorData record = new SensorData();
    record.domain = row.getString("domain");
    record.vendor = row.getString("vendor");
    record.creationDate = date(row.getDate("creation_date"));
    record.technology = row.getString("technology");
    record.uniqueIdentifier = row.getString("unique_identifier");
    record.reportingTime = dateTime(row.getTimestamp("reporting_time"));
    record.dumpProcessTime = dateTime(row.getTimestamp("dump_process_time"));
    record.senderName = row.getString("sender_name");
    record.senderIp = row.getString("sender_ip");
    record.rca = row.getString("rca");
    record.rcaId = row.getString("rca_id");
    record.rawTrap = row.getString("raw_trap");
    record.alarmExternalId = row.getString("alarm_external_id");
    record.alarmName = row.getString("alarm_name");
    record.description = row.getString("description");
    record.subEntity = row.getString("subentity");
    record.eventTime = dateTime(row.getTimestamp("event_time"));
    record.entityId = row.getString("entity_id");
    record.entityStatus = row.getString("entity_status");
    record.severity = row.getString("severity");
    record.l1ProcessTime = dateTime(row.getTimestamp("l1_process_time"));
    record.rowKey = row.getString("rowkey");
    return record;

  }

  private static LocalDate date(com.datastax.driver.core.LocalDate dt) {
    return LocalDate.ofInstant(Instant.ofEpochMilli(dt.getMillisSinceEpoch()),
      ZoneId.systemDefault());
  }

  private static LocalDateTime dateTime(Date date) {
    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
  }

}
