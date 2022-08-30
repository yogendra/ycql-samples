package com.yugabyte.samples.springbootcassnadraycql;

import java.util.Optional;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepository extends CassandraRepository<Customer, Integer> {
  Optional<Customer> findByFirstName(String firstName);

}
