package com.yugabyte.sample.apps;

import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;
import org.slf4j.Logger;

public class DataGenerator {

  private static final Logger logger = getLogger(DataGenerator.class);
  private static final int VENDOR_COUNT = 25;
  private static final int DOMAIN_COUNT = 25;
  private static final int TECH_COUNT = 5;
  private static final int RECORD_PER_COMBO = 100;


  public static void main(String[] args) {
    try {
      // Create a Cassandra client.
      Cluster cluster = Cluster.builder()
        .addContactPoint("")
        .withCredentials("cassandra", "")
        .build();
      Session session = cluster.connect();

      // Create keyspace 'ybdemo' if it does not exist.
      String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS test;";
      ResultSet createKeyspaceResult = session.execute(createKeyspace);
      System.out.println("Created keyspace test");

      // Create table 'employee' if it does not exist.
      String createTable = "CREATE TABLE IF NOT EXISTS test.sensor_data (\n"
        + "domain text,\n"
        + "vendor text,\n"
        + "creation_date date,\n"
        + "technology text,\n"
        + "unique_identifier text,\n"
        + "PRIMARY KEY ((domain, vendor, creation_date), technology, unique_identifier)\n"
        + ") WITH CLUSTERING ORDER BY (technology DESC, unique_identifier ASC)\n"
        + "AND default_time_to_live = 1728000\n"
        + "AND transactions = {'enabled': 'true'};";

      ResultSet createResult = session.execute(createTable);

      System.out.println("Created table");

      range(1, VENDOR_COUNT + 1)
        .forEach(vendorIndex -> {
          String vendor = String.format("VENDOR-%1$s", vendorIndex);
          range(1, DOMAIN_COUNT + 1).forEach(domainIndex -> {
            String domain = String.format("DOMAIN-%1$s", domainIndex);
            range(1, TECH_COUNT + 1).forEach(techIndex -> {
              String tech = String.format("TECH-%1$s", techIndex);
              logger.info("Inserting Records for {} / {} / {} / {}", vendor, domain, tech,
                RECORD_PER_COMBO);
              range(1, RECORD_PER_COMBO + 1).forEach(recIndex -> {
                // Insert a row.
                String insert = "INSERT INTO test.sensor_data "
                  + "(domain, vendor, creation_date, technology, unique_identifier)" +
                  " VALUES (?,?,toDate(now()), ?,cast( uuid() as text));";
                ResultSet insertResult = session.execute(insert, domain, vendor, tech);

              });
              logger.info("Inserted Records for {} / {} / {}/ {}", vendor, domain, tech, RECORD_PER_COMBO);
            });
          });
        });

      // Close the client.
      session.close();
      cluster.close();
      System.exit(0);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
