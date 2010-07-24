
package org.gora.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gora.GoraTestDriver;
import org.gora.examples.generated.Employee;
import org.gora.examples.generated.WebPage;
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
      employeeStore = testDriver.createDataStore(String.class, Employee.class);
      webPageStore = testDriver.createDataStore(String.class, WebPage.class);
      testDriver.setUp();
    } else {
      employeeStore =  createEmployeeDataStore();
      webPageStore = createWebPageDataStore();

      employeeStore.truncateSchema();
      webPageStore.truncateSchema();
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
    log.info("test method: testNewInstance");
    DataStoreTestUtil.testNewPersistent(employeeStore);
  }

  @Test
  public void testCreateSchema() throws Exception {
    log.info("test method: testCreateSchema");
    DataStoreTestUtil.testCreateEmployeeSchema(employeeStore);
    assertSchemaExists("Employee");
  }

  // Override this to assert that schema is created correctly
  public void assertSchemaExists(String schemaName) throws Exception {
  }

  @Test
  public void testAutoCreateSchema() throws Exception {
    log.info("test method: testAutoCreateSchema");
    DataStoreTestUtil.testAutoCreateSchema(employeeStore);
    assertAutoCreateSchema();
  }

  public void assertAutoCreateSchema() throws Exception {
    assertSchemaExists("Employee");
  }

  @Test
  public  void testTruncateSchema() throws Exception {
    log.info("test method: testTruncateSchema");
    DataStoreTestUtil.testTruncateSchema(webPageStore);
    assertSchemaExists("WebPage");
  }

  @Test
  public void testDeleteSchema() throws IOException {
    log.info("test method: testDeleteSchema");
    DataStoreTestUtil.testDeleteSchema(webPageStore);
  }

  @Test
  public void testSchemaExists() throws Exception {
    log.info("test method: testSchemaExists");
    DataStoreTestUtil.testSchemaExists(webPageStore);
    assertSchemaExists("WebPage");
  }

  @Test
  public void testPut() throws IOException {
    log.info("test method: testPut");
    Employee employee = DataStoreTestUtil.testPutEmployee(employeeStore);
    assertPut(employee);
  }

  public void assertPut(Employee employee) throws IOException {
  }

  @Test
  public void testPutArray() throws IOException {
    log.info("test method: testPutArray");
    webPageStore.createSchema();
    WebPage page = webPageStore.newPersistent();
    
    String[] tokens = {"example", "content", "in", "example.com"};
    
    for(String token: tokens) {
      page.addToParsedContent(new Utf8(token));  
    }
    
    webPageStore.put("com.example/http", page);
    webPageStore.close();
    
    assertPutArray();
  }
  
  public void assertPutArray() throws IOException {
  }
  
  @Test
  public void testPutBytes() throws IOException {
    log.info("test method: testPutBytes");
    webPageStore.createSchema();
    WebPage page = webPageStore.newPersistent();
    page.setUrl(new Utf8("http://example.com"));
    byte[] contentBytes = "example content in example.com".getBytes();
    ByteBuffer buff = ByteBuffer.wrap(contentBytes);
    page.setContent(buff);
    
    webPageStore.put("com.example/http", page);
    webPageStore.close();
    
    assertPutBytes(contentBytes);
  }
  
  public void assertPutBytes(byte[] contentBytes) throws IOException {
  }
  
  @Test
  public void testPutMap() throws IOException {    
    log.info("test method: testPutMap");
    webPageStore.createSchema();
    
    WebPage page = webPageStore.newPersistent();
    
    page.setUrl(new Utf8("http://example.com"));
    page.putToOutlinks(new Utf8("http://example2.com"), new Utf8("anchor2"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor3"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor4"));
    webPageStore.put("com.example/http", page);
    webPageStore.close();
    
    assertPutMap();
  }
  
  public void assertPutMap() throws IOException {
  }
  
  @Test
  public void testUpdate() throws IOException {
    log.info("test method: testUpdate");
    DataStoreTestUtil.testUpdateEmployee(employeeStore);
    DataStoreTestUtil.testUpdateWebPage(webPageStore);
  }  

  public void testEmptyUpdate() throws IOException {
    DataStoreTestUtil.testEmptyUpdateEmployee(employeeStore);
  }

  @Test
  public void testGet() throws IOException {
    log.info("test method: testGet");
    DataStoreTestUtil.testGetEmployee(employeeStore);
  }

  @Test
  public void testGetWithFields() throws IOException {
    log.info("test method: testGetWithFields");
    DataStoreTestUtil.testGetEmployeeWithFields(employeeStore);
  }

  @Test
  public void testGetWebPage() throws IOException {
    log.info("test method: testGetWebPage");
    DataStoreTestUtil.testGetWebPage(webPageStore);
  }

  @Test
  public void testGetWebPageDefaultFields() throws IOException {
    log.info("test method: testGetWebPageDefaultFields");
    DataStoreTestUtil.testGetWebPageDefaultFields(webPageStore);
  }

  @Test 
  public void testGetNonExisting() throws Exception {
    log.info("test method: testGetNonExisting");
    DataStoreTestUtil.testGetEmployeeNonExisting(employeeStore);
  }

 @Test
  public void testQuery() throws IOException {
    log.info("test method: testQuery");
    DataStoreTestUtil.testQueryWebPages(webPageStore);
  }

  @Test
  public void testQueryStartKey() throws IOException {
    log.info("test method: testQueryStartKey");
    DataStoreTestUtil.testQueryWebPageStartKey(webPageStore);
  }

  @Test
  public void testQueryEndKey() throws IOException {
    log.info("test method: testQueryEndKey");
    DataStoreTestUtil.testQueryWebPageEndKey(webPageStore);
  }

  @Test
  public void testQueryKeyRange() throws IOException {
    log.info("test method: testQueryKetRange");
    DataStoreTestUtil.testQueryWebPageKeyRange(webPageStore);
  }

 @Test
  public void testQueryWebPageSingleKey() throws IOException {
   log.info("test method: testQueryWebPageSingleKey");
    DataStoreTestUtil.testQueryWebPageSingleKey(webPageStore);
  }

  @Test
  public void testQueryWebPageSingleKeyDefaultFields() throws IOException {
    log.info("test method: testQuerySingleKeyDefaultFields");
    DataStoreTestUtil.testQueryWebPageSingleKeyDefaultFields(webPageStore);
  }
  
  @Test
  public void testQueryWebPageQueryEmptyResults() throws IOException {
    log.info("test method: testQueryEmptyResults");
    DataStoreTestUtil.testQueryWebPageEmptyResults(webPageStore);
  }
  
  @Test
  public void testDelete() throws IOException {
    log.info("test method: testDelete");
    DataStoreTestUtil.testDelete(webPageStore);
  }

  @Test
  public void testDeleteByQuery() throws IOException {
    log.info("test method: testDeleteByQuery");
    DataStoreTestUtil.testDeleteByQuery(webPageStore);
  }

  @Test
  public void testDeleteByQueryFields() throws IOException {
    log.info("test method: testQueryByQueryFields");
    DataStoreTestUtil.testDeleteByQueryFields(webPageStore);
  }
  
  @Test
  public void testGetPartitions() throws IOException {
    log.info("test method: testGetPartitions");
    DataStoreTestUtil.testGetPartitions(webPageStore);
  }
}
