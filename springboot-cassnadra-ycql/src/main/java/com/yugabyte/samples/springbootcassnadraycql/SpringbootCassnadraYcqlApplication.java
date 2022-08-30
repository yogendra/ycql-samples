package com.yugabyte.samples.springbootcassnadraycql;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class SpringbootCassnadraYcqlApplication implements CommandLineRunner {

  final CustomerRepository customerRepository;

  final CqlSession ycqlSession;

  public SpringbootCassnadraYcqlApplication(CustomerRepository customerRepository,
    CqlSession ycqlSession) {
    this.customerRepository = customerRepository;
    this.ycqlSession = ycqlSession;
  }

  public static void main(String[] args) {
    SpringApplication.run(SpringbootCassnadraYcqlApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {

    log.info("Creating Keyspace");
    ycqlSession.execute("CREATE KEYSPACE IF NOT EXISTS demo;");
    log.info("Creating table");
    ycqlSession.execute("""
       CREATE TABLE IF NOT EXISTS demo.customer
       (
       id INT PRIMARY KEY,
       firstName text,
       lastName text
       )
       WITH default_time_to_live = 0 AND transactions = {'enabled': 'true'};
      """);

    // Split the array of whole names into an array of first-last names
    var splitUpNames = Stream.of("Tim Bun", "Mike Dean", "Alan Row", "Josh Rambo")
      .map(name -> name.split(" "))
      .collect(Collectors.toList());

    // Use a Java 8 stream to print out each tuple of the list

    IntStream.range(0, splitUpNames.size())
      .forEach(i -> {
        int id = i + 1;
        var record = splitUpNames.get(i);
        log.info(String.format("Inserting customer record for %s %s", record[0], record[1]));
        customerRepository.save(new Customer( id, (String) record[0], (String) record[1]));
      });
    log.info("Querying for customer records where first_name = 'Josh':");
    log.info(customerRepository.findByFirstName("Josh")
      .toString());


  }
}
