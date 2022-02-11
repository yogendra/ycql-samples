package com.yugabyte.sample.apps;

import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class YBCqlHelloWorld {

  public static void main(String[] args) {
    try {
      // Create a Cassandra client.
      Cluster cluster = Cluster.builder()
        .addContactPoint("")
        .withCredentials("cassandra","")
        .build();
      Session session = cluster.connect();

      // Create keyspace 'ybdemo' if it does not exist.
      String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS ybdemo;";
      ResultSet createKeyspaceResult = session.execute(createKeyspace);
      System.out.println("Created keyspace ybdemo");

      // Create table 'employee' if it does not exist.
      String createTable = "CREATE TABLE IF NOT EXISTS ybdemo.employee (id int PRIMARY KEY, " +
        "name varchar, " +
        "age int, " +
        "language varchar);";
      ResultSet createResult = session.execute(createTable);
      System.out.println("Created table employee");

      // Insert a row.
      String insert = "INSERT INTO ybdemo.employee (id, name, age, language)" +
        " VALUES (1, 'John', 35, 'Java');";
      ResultSet insertResult = session.execute(insert);
      System.out.println("Inserted data: " + insert);

      // Query the row and print out the result.
      String select = "SELECT name, age, language FROM ybdemo.employee WHERE id = 1;";
      ResultSet selectResult = session.execute(select);
      List<Row> rows = selectResult.all();
      String name = rows.get(0)
        .getString(0);
      int age = rows.get(0)
        .getInt(1);
      String language = rows.get(0)
        .getString(2);
      System.out.println("Query returned " + rows.size() + " row: " +
        "name=" + name + ", age=" + age + ", language: " + language);

      // Close the client.
      session.close();
      cluster.close();
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
