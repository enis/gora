
package org.gora.store.hbase;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gora.example.generated.Employee;
import org.gora.hbase.store.HBaseStore;
import org.junit.Before;
import org.junit.Test;

public class TestHBaseStore extends HBaseClusterTestCase {
 
  private HBaseConfiguration conf = new HBaseConfiguration();
  private HBaseStore<String, Employee> store;
  
  private static final long YEAR_IN_MS = 365L * 24L * 60L * 60L * 1000L; 
  
  private static final String[] EMPYLOYEE_FIELDS 
    = {"name", "dateOfBirth", "ssn", "salary"};
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.store = new HBaseStore<String, Employee>(conf, String.class, 
        Employee.class);
  }
  
  private Employee createEmployee() throws IOException {
    Employee employee = store.newInstance();
    employee.setName(new Utf8("Random Joe"));
    employee.setDateOfBirth( System.currentTimeMillis() - 42L *  YEAR_IN_MS );
    employee.setSalary(100000);
    employee.setSsn(new Utf8("101010101010"));
    return employee;
  }
  
  @Test
  public void testNewInstance() throws IOException {
    createEmployee();
  }
  
  public void testCreateTable() throws IOException {
    store.createTable();
  }
  
  public void testPut() throws IOException {
    store.createTable();
    Employee employee = createEmployee();
    store.put(employee.getSsn().toString(), employee);
  }
  
  public void testGet() throws IOException {
    store.createTable();
    Employee employee = createEmployee();
    String ssn = employee.getSsn().toString();
    store.put(ssn, employee);
    store.flush();
    
    Employee after = store.get(ssn, EMPYLOYEE_FIELDS);
    
    Assert.assertEquals(employee, after);
  }
  
}
