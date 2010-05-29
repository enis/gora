
package org.gora.hbase.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;
import org.gora.store.DataStoreTestUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for HBaseStore.
 */
public class TestHBaseStore extends HBaseClusterTestCase {

  private HBaseStore<String, Employee> employeeStore;
  private HBaseStore<String, WebPage> webPageStore;
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
   employeeStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, Employee.class);
  
   webPageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
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
    HBaseAdmin admin = new HBaseAdmin(conf);
    Assert.assertTrue(admin.tableExists("Employee"));
  }
  
  @Test
  public void testAutoCreateSchema() throws IOException {
    //should not throw exception
    employeeStore.put("foo", new Employee());
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
    DataStoreTestUtil.testPutEmployee(employeeStore);
  }
  
  @Test
  public void testGet() throws IOException {
    DataStoreTestUtil.testGetEmployee(employeeStore);
  }
 
  @Test 
  public void testGetNonExisting() throws Exception {
    DataStoreTestUtil.testGetEmployeeNonExisting(employeeStore);
  }
  
  @Test
  public void testPutArray() throws IOException {
    DataStore<String,WebPage> pageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
    
    pageStore.createSchema();
    WebPage page = pageStore.newPersistent();
    
    String[] tokens = {"example", "content", "in", "example.com"};
    
    for(String token: tokens) {
      page.addToParsedContent(new Utf8(token));  
    }
    
    pageStore.put("com.example/http", page);
    pageStore.close();
    
    HTable table = new HTable("WebPage");
    Get get = new Get(Bytes.toBytes("com.example/http"));
    org.apache.hadoop.hbase.client.Result result = table.get(get);
    
    Assert.assertEquals(result.getFamilyMap(Bytes.toBytes("parsedContent")).size(), 4);
    Assert.assertTrue(Arrays.equals(result.getValue(Bytes.toBytes("parsedContent")
        ,Bytes.toBytes(0)), Bytes.toBytes("example")));
    
    Assert.assertTrue(Arrays.equals(result.getValue(Bytes.toBytes("parsedContent")
        ,Bytes.toBytes(3)), Bytes.toBytes("example.com")));
    table.close();
    
    deleteTable("WebPage");
  }
  
  @Test
  public void testPutBytes() throws IOException {
    DataStore<String,WebPage> pageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
    
    pageStore.createSchema();
    WebPage page = pageStore.newPersistent();
    page.setUrl(new Utf8("http://example.com"));
    byte[] contentBytes = "example content in example.com".getBytes();
    ByteBuffer buff = ByteBuffer.wrap(contentBytes);
    page.setContent(buff);
    
    pageStore.put("com.example/http", page);
    pageStore.close();
    
    HTable table = new HTable("WebPage");
    Get get = new Get(Bytes.toBytes("com.example/http"));
    org.apache.hadoop.hbase.client.Result result = table.get(get);
    
    byte[] actualBytes = result.getValue(Bytes.toBytes("content"), null);
    assertNotNull(actualBytes);
    Assert.assertTrue(Arrays.equals(contentBytes, actualBytes));
    table.close();
    
    deleteTable("WebPage");
  }
  
  @Test
  public void testPutMap() throws IOException {
    DataStore<String,WebPage> pageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
    
    pageStore.createSchema();
    
    WebPage page = pageStore.newPersistent();
    
    page.setUrl(new Utf8("http://example.com"));
    page.putToOutlinks(new Utf8("http://example2.com"), new Utf8("anchor2"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor3"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor4"));
    pageStore.put("com.example/http", page);
    pageStore.close();
    
    HTable table = new HTable("WebPage");
    Get get = new Get(Bytes.toBytes("com.example/http"));
    org.apache.hadoop.hbase.client.Result result = table.get(get);
    
    byte[] anchor2Raw = result.getValue(Bytes.toBytes("outlinks")
        , Bytes.toBytes("http://example2.com"));
    Assert.assertNotNull(anchor2Raw);
    String anchor2 = Bytes.toString(anchor2Raw);
    Assert.assertEquals("anchor2", anchor2);
    table.close();
    
    deleteTable("WebPage");
  }
  
  private void deleteTable(String tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
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

  @Test
  public void testQueryWebPageQueryEmptyResults() throws IOException {
    DataStoreTestUtil.testQueryWebPageQueryEmptyResults(webPageStore);
  }

  @Test
  public void testGetPartitions() throws IOException {
    DataStoreTestUtil.testGetPartitions(webPageStore);
  }
  
  @Test
  public void testDeleteByQuery() throws IOException {
    DataStoreTestUtil.testDeleteByQuery(webPageStore);
  }
}
