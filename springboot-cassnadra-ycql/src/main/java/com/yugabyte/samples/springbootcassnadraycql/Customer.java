package com.yugabyte.samples.springbootcassnadraycql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Table("customer")
public class Customer {
  @Id
  private Integer id;
  private String firstName;
  private String lastName;



}
