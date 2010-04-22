
package org.gora.store.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gora.example.generated.Employee;
import org.gora.hbase.store.HBaseStore;
import org.gora.store.DataStoreTestUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for HBaseStore.
 */
public class TestHBaseStore extends HBaseClusterTestCase {
 
  private HBaseConfiguration conf = new HBaseConfiguration();
  private HBaseStore<String, Employee> store;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.store = new HBaseStore<String, Employee>(conf, String.class, 
        Employee.class);
  }

  @Test
  public void testNewInstance() throws IOException {
    DataStoreTestUtil.testNewInstance(store);
  }
  
  public void testCreateTable() throws IOException {
    store.createTable();
  }
  
  public void testPut() throws IOException {
    DataStoreTestUtil.testPutEmployee(store);
  }
  
  public void testGet() throws IOException {
    DataStoreTestUtil.testGetEmployee(store);
  }
  
}
