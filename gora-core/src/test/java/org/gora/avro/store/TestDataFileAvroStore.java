
package org.gora.avro.store;

import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;

/**
 * Test case for {@link DataFileAvroStore}.
 */
public class TestDataFileAvroStore extends TestAvroStore {

  @Override
  protected AvroStore<String, Employee> createEmployeeDataStore() {
    return new DataFileAvroStore<String, Employee>();
  }
  
  @Override
  protected AvroStore<String, WebPage> createWebPageDataStore() {
    return new DataFileAvroStore<String, WebPage>();
  }
  
  //import all tests from super class
  
}
