
package org.gora.store;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.avro.util.Utf8;
import org.gora.example.generated.Employee;
import org.gora.persistency.Persistent;

/**
 * Test utilities for DataStores
 */
public class DataStoreTestUtil {

  public static final long YEAR_IN_MS = 365L * 24L * 60L * 60L * 1000L;
  
  public static final String[] EMPYLOYEE_FIELDS 
    = {"name", "dateOfBirth", "ssn", "salary"};
  
  public static <K, T extends Persistent> void testNewInstance(
      DataStore<K,T> dataStore) throws IOException {
    
    T obj1 = dataStore.newInstance();
    T obj2 = dataStore.newInstance();
    
    Assert.assertNotNull(obj1);
    Assert.assertNotNull(obj2);
    Assert.assertFalse( obj1 == obj2 );
  }
  
  public static <K> Employee createEmployee(
      DataStore<K, Employee> dataStore) throws IOException {
    
    Employee employee = dataStore.newInstance();
    employee.setName(new Utf8("Random Joe"));
    employee.setDateOfBirth( System.currentTimeMillis() - 42L *  YEAR_IN_MS );
    employee.setSalary(100000);
    employee.setSsn(new Utf8("101010101010"));
    return employee;
  }
  
  public static void testGetEmployee(DataStore<String, Employee> dataStore) 
    throws IOException {
    dataStore.createTable();
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    String ssn = employee.getSsn().toString();
    dataStore.put(ssn, employee);
    dataStore.flush();
    
    Employee after = dataStore.get(ssn, DataStoreTestUtil.EMPYLOYEE_FIELDS);
    
    Assert.assertEquals(employee, after);
  }
  
  public static void testPutEmployee(DataStore<String, Employee> dataStore) 
  throws IOException {
    dataStore.createTable();
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    dataStore.put(employee.getSsn().toString(), employee);   
  }
  
}
