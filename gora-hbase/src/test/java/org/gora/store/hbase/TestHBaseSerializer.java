
package org.gora.store.hbase;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gora.example.generated.Employee;
import org.junit.Before;
import org.junit.Test;

public class TestHBaseSerializer extends HBaseClusterTestCase {
 
  private HBaseConfiguration conf = new HBaseConfiguration();
  private HbaseSerializer<String, Employee> serializer;
  
  private static final long YEAR_IN_MS = 365L * 24L * 60L * 60L * 1000L; 
  
  private static final String[] EMPYLOYEE_FIELDS 
    = {"name", "dateOfBirth", "ssn", "salary"};
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.serializer = new HbaseSerializer<String, Employee>(conf, String.class, 
        Employee.class);
  }
  
  private Employee createEmployee() throws IOException {
    Employee employee = serializer.makeRow();
    employee.setName(new Utf8("Random Joe"));
    employee.setDateOfBirth( System.currentTimeMillis() - 42L *  YEAR_IN_MS );
    employee.setSalary(100000);
    employee.setSsn(new Utf8("101010101010"));
    return employee;
  }
  
  @Test
  public void testMakeRow() throws IOException {
    createEmployee();
  }
  
  public void testCreateTable() throws IOException {
    serializer.createTable();
  }
  
  public void testUpdateRow() throws IOException {
    serializer.createTable();
    Employee employee = createEmployee();
    serializer.updateRow(employee.getSsn().toString(), employee);
  }
  
  public void testReadRow() throws IOException {
    serializer.createTable();
    Employee employee = createEmployee();
    String ssn = employee.getSsn().toString();
    serializer.updateRow(ssn, employee);
    serializer.sync();
    
    Employee after = serializer.readRow(ssn, EMPYLOYEE_FIELDS);
    
    Assert.assertEquals(employee, after);
  }
  
}
