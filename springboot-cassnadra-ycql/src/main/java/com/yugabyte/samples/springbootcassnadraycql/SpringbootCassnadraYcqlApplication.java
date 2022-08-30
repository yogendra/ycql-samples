package com.yugabyte.samples.springbootcassnadraycql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
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

//  static{
//    System.setProperty("datastax-java-driver.advanced.request-tracker.class", "RequestLogger");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.success.enabled", "true");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.enabled", "true");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.error.enabled", "true");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.show-values", "true");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.max-value-length", "100");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.max-values", "100");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.threshold ", "1 second");
//    System.setProperty("datastax-java-driver.advanced.request-tracker.logs.show-stack-trace", "true");
//  }
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

    logSessionInfo();
    createSchema();
    populateData();
    tryQuery();


  }

  private void tryQuery() {
    log.info("Customer Count: {}", customerRepository.countAll());
  }

  private void populateData() {
    // Split the array of whole names into an array of first-last names
    var splitUpNames = Stream.of("Tim Bun", "Mike Dean", "Alan Row", "Josh Rambo")
      .map(name -> name.split(" "))
      .toList();

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

  private void createSchema() {
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
  }

  private void logSessionInfo() {
    var context = ycqlSession.getContext();
    var config = context.getConfig();
    log.info("YCQL Session Info / Session Name: {}", context.getSessionName());
    logSessionConfigInfo("context.config", config);
    logSessionConfigInfo("context.initConfig", context.getConfigLoader().getInitialConfig());

  }

  private void logSessionConfigInfo(String configName, DriverConfig config) {
    log.info("Config Name: {}", configName);
    config.getProfiles().forEach(this::logProfileInfo);
  }

  private void logProfileInfo(String profileName, DriverExecutionProfile profile) {
    log.info("YCQL Session Info / Profile Name: {}", profile.getName());
    log.info("YCQL Session Info / Connection Timeout:{} ", profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT));
    log.info("YCQL Session Info / Init query Timeout:{} ", profile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT));
    log.info("YCQL Session Info / Request Timeout:{} ", profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT));
    log.info("YCQL Session Info / Control Connection Timeout:{} ", profile.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT));
  }
}
