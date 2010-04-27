
package org.gora.store.hbase;

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
import org.gora.hbase.store.HBaseStore;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;
import org.gora.store.DataStoreTestUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test case for HBaseStore.
 */
public class TestHBaseStore extends HBaseClusterTestCase {
 
  private HBaseStore<String, Employee> store;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.store = (HBaseStore<String, Employee>) DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, Employee.class);    
  }

  @Test
  public void testNewInstance() throws IOException {
    DataStoreTestUtil.testNewInstance(store);
  }
  
  @Test
  public void testCreateTable() throws IOException {
    store.createTable();
  }
  
  @Test
  public void testPut() throws IOException {
    DataStoreTestUtil.testPutEmployee(store);
  }
  
  @Test
  public void testGet() throws IOException {
    DataStoreTestUtil.testGetEmployee(store);
  }
 
  @Test
  @Ignore
  public void testPutArray() throws IOException {
    DataStore<String,WebPage> pageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
    
    WebPage page = pageStore.newInstance();
  }
  
  @Test
  public void testPutBytes() throws IOException {
    DataStore<String,WebPage> pageStore = DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
    
    pageStore.createTable();
    WebPage page = pageStore.newInstance();
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
    
    pageStore.createTable();
    
    WebPage page = pageStore.newInstance();
    
    page.setUrl(new Utf8("http://example.com"));
    page.putToOutlinks(new Utf8("http://example2.com"), new Utf8("anchor2"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor3"));
    page.putToOutlinks(new Utf8("http://example3.com"), new Utf8("anchor4"));
    pageStore.put("com.example/http", page);
    pageStore.close();
    
    HTable table = new HTable("WebPage");
    Get get = new Get(Bytes.toBytes("com.example/http"));
    org.apache.hadoop.hbase.client.Result result = table.get(get);
    
    byte[] anchor2Raw = result.getValue(Bytes.toBytes("outlinks"), Bytes.toBytes("http://example2.com"));
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
  
}
