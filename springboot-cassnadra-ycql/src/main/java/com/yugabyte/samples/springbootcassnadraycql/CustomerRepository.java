package com.yugabyte.samples.springbootcassnadraycql;

import java.util.Optional;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepository extends CassandraRepository<Customer, Integer> {
  Optional<Customer> findByFirstName(String firstName);

  @Query("SELECT count(id) from customer")
  long countAll();

}
