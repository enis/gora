
package org.gora.store;

import java.io.IOException;

import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.junit.Before;
import org.junit.Test;

/**
 * A base class for {@link DataStore} tests. This is just a convenience
 * class, which actually only uses {@link DataStoreTestUtil} methods to 
 * run the tests. Not all test cases can extend this class (like TestHBaseStore), 
 * so all test logic shuold reside in DataStoreTestUtil class.  
 */
public abstract class DataStoreTestBase {

  protected DataStore<String,Employee> employeeStore;
  protected DataStore<String,WebPage> webPageStore;

  @Before
  public void setUp() throws Exception {
    employeeStore =  createEmployeeDataStore();
    webPageStore = createWebPageDataStore();
  }

  protected abstract DataStore<String,Employee> createEmployeeDataStore();
  
  protected abstract DataStore<String,WebPage> createWebPageDataStore();
  
  protected void tearDown() throws Exception {
    employeeStore.close();
    webPageStore.close();
  }

  @Test
  public void testNewInstance() throws IOException {
    DataStoreTestUtil.testNewPersistent(employeeStore);
  }
  
  @Test
  public void testCreateSchema() throws IOException {
    DataStoreTestUtil.testCreateEmployeeSchema(employeeStore);
    assertCreateSchema();
  }

  /** Override this to assert that schema is created correctly */
  public void assertCreateSchema() throws IOException {
  }
  
  @Test
  public void testAutoCreateSchema() throws IOException {
    DataStoreTestUtil.testAutoCreateSchema(employeeStore);
    assertAutoCreateSchema();
  }

  public void assertAutoCreateSchema() throws IOException {
    assertCreateSchema();
  }
  
  @Test
  public void testPut() throws IOException {
    Employee employee = DataStoreTestUtil.testPutEmployee(employeeStore);
    assertPut(employee);
  }

  public void assertPut(Employee employee) throws IOException {
  }
  
  @Test
  public void testGet() throws IOException {
    DataStoreTestUtil.testGetEmployee(employeeStore);
  }
  
  @Test
  public void testGetWithFields() throws IOException {
    DataStoreTestUtil.testGetEmployeeWithFields(employeeStore);
  }
  
  @Test
  public void testGetWebPage() throws IOException {
    DataStoreTestUtil.testGetWebPage(webPageStore);
  }
  
  @Test
  public void testGetWebPageDefaultFields() throws IOException {
    DataStoreTestUtil.testGetWebPageDefaultFields(webPageStore);
  }
  
  @Test
  public void testQueryWebPageSingleKey() throws IOException {
    DataStoreTestUtil.testQueryWebPageSingleKey(webPageStore);
  }
  
  @Test
  public void testQueryWebPageSingleKeyDefaultFields() throws IOException { 
    DataStoreTestUtil.testQueryWebPageSingleKeyDefaultFields(webPageStore);
  }
  
  @Test
  public void testQuery() throws IOException {
    DataStoreTestUtil.testQueryWebPages(webPageStore);
  }
  
  @Test
  public void testQueryStartKey() throws IOException {
    DataStoreTestUtil.testQueryWebPageStartKey(webPageStore);
  }
  
  @Test
  public void testQueryEndKey() throws IOException {
    DataStoreTestUtil.testQueryWebPageEndKey(webPageStore);
  }
  
  @Test
  public void testQueryKeyRange() throws IOException {
    DataStoreTestUtil.testQueryWebPageKeyRange(webPageStore);
  }
  
}
