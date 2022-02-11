package com.yugabyte.sample.apps;

import static java.util.stream.IntStream.range;
import static org.slf4j.LoggerFactory.getLogger;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
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

      CqlSession session = CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
        .withLocalDatacenter("datacenter1")
        .build();


      // Create keyspace 'ybdemo' if it does not exist.
      String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS test;";
      session.execute(createKeyspace);


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

      session.execute(createTable);


      System.out.println("Created table");
      String insertString = "INSERT INTO test.sensor_data "
        + "(domain, vendor, creation_date, technology, unique_identifier)" +
        " VALUES (?,?,toDate(now()), ?,cast( uuid() as text));";
      PreparedStatement insert  = session.prepare(insertString);

      for( var vendorIndex = 1; vendorIndex <= VENDOR_COUNT; ++vendorIndex ){
        var vendor =  String.format("VENDOR-%1$s", vendorIndex);
        for (var domainIndex = 1; domainIndex <= DOMAIN_COUNT ; ++domainIndex){
          var domain = String.format("DOMAIN-%1$s", domainIndex);
          for (var techIndex = 1 ; techIndex <= TECH_COUNT; ++techIndex){
            var tech = String.format("TECH-%1$s", techIndex);
            for (var recIndex = 1; recIndex <= RECORD_PER_COMBO; ++recIndex){
              ResultSet selectResult = session.execute(insert.bind(domain,vendor,tech) );
            }
          }
        }
      }

      // Close the client.
      session.close();
      System.exit(0);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
