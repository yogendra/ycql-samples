package com.yugabyte.samples.springbootcassnadraycql;


import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/customers")
public class CustomerController {


  private final CustomerRepository customerRepository;

  public CustomerController(CustomerRepository customerRepository) {
    this.customerRepository = customerRepository;
  }

  @GetMapping()
  public List<Customer> list() {
    return customerRepository.findAll();
  }

  @PostMapping()
  public Customer save(Customer customer) {
    return customerRepository.save(customer);
  }

  @GetMapping("/{customerId}")
  public Customer get(@PathVariable Integer customerId){
    return customerRepository.findById(customerId)
      .orElseThrow(()->new CustoemrNotFoundException(customerId));
  }

  @DeleteMapping("/{customerId}")
  public String deleteProductById(@PathVariable Integer customerId) {
    customerRepository.deleteById(customerId);
    return "Delete Success";
  }

  private class CustoemrNotFoundException extends RuntimeException{

    public CustoemrNotFoundException(Integer customerId) {
      super(String.format("{}:Not Found",customerId));
    }

  }
}
