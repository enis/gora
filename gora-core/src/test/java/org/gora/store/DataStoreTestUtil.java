
package org.gora.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.query.Result;

/**
 * Test utilities for DataStores
 */
public class DataStoreTestUtil {

  public static final long YEAR_IN_MS = 365L * 24L * 60L * 60L * 1000L;
  
  public static final String[] EMPYLOYEE_FIELDS 
    = {"name", "dateOfBirth", "ssn", "salary"};
  
  public static final String[] URLS = {
    "http://foo.com/",
    "http://foo.com/1.html",
    "http://foo.com/2.html",
    "http://bar.com/3.jsp",
    "http://bar.com/1.html",
    "http://bar.com/",
    "http://baz.com/1.jsp&q=barbaz",
    "http://baz.com/1.jsp&q=barbaz&p=foo",
    "http://baz.com/1.jsp&q=foo",
    "http://bazbar.com",
  };
  
  public static final String[] CONTENTS = {
    "foo baz bar",
    "foo",
    "foo1 bar1 baz1",
    "a b c d e",
    "aa bb cc dd ee",
    "1",
    "2 3",
    "a b b b b b a",
    "a a a",
    "foo bar baz",
  };
  
  public static final int[][] LINKS = {
    {1, 2, 3, 9},
    {3, 9},
    {},
    {9},
    {5},
    {1, 2, 3, 4, 6, 7, 8, 9},
    {1},
    {2},
    {3},
    {8, 1},
  };
  
  public static final String[][] ANCHORS = {
    {"foo", "foo", "foo", "foo"},
    {"a1", "a2"},
    {},
    {"anchor1"},
    {"bar"},
    {"a1", "a2", "a3", "a4","a5", "a6", "a7", "a8", "a9"},
    {"foo"},
    {"baz"},
    {"bazbar"},
    {"baz", "bar"},
  };
  
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
  
  public static void testCreateEmployeeSchema(DataStore<String, Employee> dataStore) 
  throws IOException {
    dataStore.createSchema();
    
    //should not throw exception
    dataStore.createSchema();
  }
  
  public static void testGetEmployee(DataStore<String, Employee> dataStore) 
    throws IOException {
    dataStore.createSchema();
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    String ssn = employee.getSsn().toString();
    dataStore.put(ssn, employee);
    dataStore.flush();
    
    Employee after = dataStore.get(ssn, DataStoreTestUtil.EMPYLOYEE_FIELDS);
    
    Assert.assertEquals(employee, after);
  }
  
  public static void testPutEmployee(DataStore<String, Employee> dataStore) 
  throws IOException {
    dataStore.createSchema();
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    dataStore.put(employee.getSsn().toString(), employee);   
  }
  
  public static void createWebPageData(DataStore<String, WebPage> dataStore) 
  throws IOException {
    WebPage page;
    
    dataStore.createSchema();
    
    for(int i=0; i<URLS.length; i++) {
      page = new WebPage();
      page.setUrl(new Utf8(URLS[i]));
      page.setContent(ByteBuffer.wrap(CONTENTS[i].getBytes()));
      for(String token : CONTENTS[i].split(" ")) {
        page.addToParsedContent(new Utf8(token));  
      }
      
      for(int j=0; j<LINKS[i].length; j++) {
        page.putToOutlinks(new Utf8(URLS[LINKS[i][j]]), new Utf8(ANCHORS[i][j]));
      }
      
      dataStore.put(URLS[i], page);
      dataStore.flush();
    }
  }
  
  private static void assertWebPage(WebPage page, int i) {
    Assert.assertNotNull(page);
    
    Assert.assertEquals(page.getUrl().toString(), URLS[i]);
    Assert.assertTrue(Arrays.equals(page.getContent().array()
        , CONTENTS[i].getBytes()));
    
    GenericArray<Utf8> parsedContent = page.getParsedContent();
    Assert.assertNotNull(parsedContent);
    Assert.assertTrue(parsedContent.size() > 0);
    
    int j=0;
    String[] tokens = CONTENTS[i].split(" ");
    for(Utf8 token : parsedContent) {
      Assert.assertEquals(tokens[j++], token.toString());
    }
    
    if(LINKS[i].length > 0) {
      Assert.assertNotNull(page.getOutlinks());
      Assert.assertTrue(page.getOutlinks().size() > 0);
      for(j=0; j<LINKS[i].length; j++) {
        Assert.assertEquals(ANCHORS[i][j], 
            page.getFromOutlinks(new Utf8(URLS[LINKS[i][j]])).toString());
      }  
    } else {
      Assert.assertTrue(page.getOutlinks() == null || page.getOutlinks().isEmpty());
    }
  }
  
  private static void testGetWebPage(DataStore<String, WebPage> store, String[] fields) 
    throws IOException {
    createWebPageData(store);
    
    for(int i=0; i<URLS.length; i++) {
      WebPage page = store.get(URLS[i], fields);
      assertWebPage(page, i);
    }
  }
  
  public static void testGetWebPage(DataStore<String, WebPage> store) throws IOException {
    testGetWebPage(store, WebPage._ALL_FIELDS);
  }
  
  public static void testGetWebPageDefaultFields(DataStore<String, WebPage> store) 
  throws IOException {
    testGetWebPage(store, null);
  }
  
  private static void testQueryWebPageSingleKey(DataStore<String, WebPage> store
      , String[] fields) throws IOException {
    
    createWebPageData(store);
    
    for(int i=0; i<URLS.length; i++) {
      Query<String, WebPage> query = store.newQuery();
      query.setFields(fields);
      query.setKey(URLS[i]);
      Result<String, WebPage> result = query.execute();
      Assert.assertTrue(result.next());
      WebPage page = result.get();
      assertWebPage(page, i);
      Assert.assertFalse(result.next());  
    }
  }
  
  public static void testQueryWebPageSingleKey(DataStore<String, WebPage> store) 
  throws IOException {
    testQueryWebPageSingleKey(store, WebPage._ALL_FIELDS);
  }
  
  public static void testQueryWebPageSingleKeyDefaultFields(
      DataStore<String, WebPage> store) throws IOException { 
    testQueryWebPageSingleKey(store, null);
  }
}
