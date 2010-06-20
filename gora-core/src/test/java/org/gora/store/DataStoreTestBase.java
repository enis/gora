
package org.gora.store;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A base class for {@link DataStore} tests. This is just a convenience
 * class, which actually only uses {@link DataStoreTestUtil} methods to
 * run the tests. Not all test cases can extend this class (like TestHBaseStore),
 * so all test logic shuold reside in DataStoreTestUtil class.
 */
public abstract class DataStoreTestBase {

  public static final Log log = LogFactory.getLog(DataStoreTestBase.class);

  protected static GoraTestDriver testDriver;

  protected DataStore<String,Employee> employeeStore;
  protected DataStore<String,WebPage> webPageStore;

  @Deprecated
  protected abstract DataStore<String,Employee> createEmployeeDataStore() throws IOException ;

  @Deprecated
  protected abstract DataStore<String,WebPage> createWebPageDataStore() throws IOException;

  /** junit annoyingly forces BeforeClass to be static, so this method
   * should be called from a static block
   */
  protected static void setTestDriver(GoraTestDriver driver) {
    testDriver = driver;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    if(testDriver != null) {
      log.info("setting up class");
      testDriver.setUpClass();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if(testDriver != null) {
      log.info("tearing down class");
      testDriver.tearDownClass();
    }
  }

  @Before
  public void setUp() throws Exception {
    log.info("setting up test");
    if(testDriver != null) {
      testDriver.setUp();
      employeeStore = testDriver.createDataStore(String.class, Employee.class);
      webPageStore = testDriver.createDataStore(String.class, WebPage.class);
    } else {
      employeeStore =  createEmployeeDataStore();
      webPageStore = createWebPageDataStore();
    }
  }

  @After
  public void tearDown() throws Exception {
    log.info("tearing down test");
    if(testDriver != null) {
      testDriver.tearDown();
    }
    //employeeStore.close();
    //webPageStore.close();
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
  public  void testTruncateSchema() throws IOException {
    DataStoreTestUtil.testTruncateSchema(webPageStore);
  }

  @Test
  public void testDeleteSchema() throws IOException {
    DataStoreTestUtil.testDeleteSchema(webPageStore);
  }

  @Test
  public void testSchemaExists() throws IOException {
    DataStoreTestUtil.testSchemaExists(webPageStore);
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
