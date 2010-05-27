
package org.gora.store;

import static org.gora.example.WebPageDataCreator.ANCHORS;
import static org.gora.example.WebPageDataCreator.CONTENTS;
import static org.gora.example.WebPageDataCreator.LINKS;
import static org.gora.example.WebPageDataCreator.URLS;
import static org.gora.example.WebPageDataCreator.URL_INDEXES;
import static org.gora.example.WebPageDataCreator.createWebPageData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.util.StringUtils;

/**
 * Test utilities for DataStores
 */
public class DataStoreTestUtil {

  public static final long YEAR_IN_MS = 365L * 24L * 60L * 60L * 1000L;
  
  public static <K, T extends Persistent> void testNewPersistent(
      DataStore<K,T> dataStore) throws IOException {
    
    T obj1 = dataStore.newPersistent();
    T obj2 = dataStore.newPersistent();
    
    Assert.assertEquals(dataStore.getPersistentClass(), 
        obj1.getClass());
    Assert.assertNotNull(obj1);
    Assert.assertNotNull(obj2);
    Assert.assertFalse( obj1 == obj2 );
  }
  
  public static <K> Employee createEmployee(
      DataStore<K, Employee> dataStore) throws IOException {
    
    Employee employee = dataStore.newPersistent();
    employee.setName(new Utf8("Random Joe"));
    employee.setDateOfBirth( System.currentTimeMillis() - 42L *  YEAR_IN_MS );
    employee.setSalary(100000);
    employee.setSsn(new Utf8("101010101010"));
    return employee;
  }
  
  public static void testAutoCreateSchema(DataStore<String,Employee> dataStore) 
  throws IOException {
    //should not throw exception
    dataStore.put("foo", createEmployee(dataStore));
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
    
    Employee after = dataStore.get(ssn, Employee._ALL_FIELDS);
    
    Assert.assertEquals(employee, after);
  }
  
  public static void testGetEmployeeNonExisting(DataStore<String, Employee> dataStore) 
    throws IOException {
    Employee employee = dataStore.get("_NON_EXISTING_SSN_FOR_EMPLOYEE_");
    Assert.assertNull(employee);
  }
  
  public static void testGetEmployeeWithFields(DataStore<String, Employee> dataStore) 
    throws IOException {
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    String ssn = employee.getSsn().toString();
    dataStore.put(ssn, employee);
    dataStore.flush();
    
    String[] fields = employee.getFields();
    for(Set<String> subset : StringUtils.powerset(fields)) {
      Employee after = dataStore.get(ssn, subset.toArray(new String[subset.size()]));
      Employee expected = new Employee();
      for(String field:subset) {
        int index = expected.getFieldIndex(field);
        expected.put(index, employee.get(index));
      }
      
      Assert.assertEquals(expected, after);
    }
  }
  
  public static Employee testPutEmployee(DataStore<String, Employee> dataStore) 
  throws IOException {
    dataStore.createSchema();
    Employee employee = DataStoreTestUtil.createEmployee(dataStore);
    dataStore.put(employee.getSsn().toString(), employee);
    return employee;
  }
  
  public static void assertWebPage(WebPage page, int i) {
    Assert.assertNotNull(page);
    
    Assert.assertEquals(URLS[i], page.getUrl().toString());
    Assert.assertTrue("content error:" + new String(page.getContent().array()) + 
        " actual=" + CONTENTS[i] + " i=" + i
    , Arrays.equals(page.getContent().array()
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
    
  public static void testQueryWebPageKeyRange(DataStore<String, WebPage> store, 
      boolean setStartKeys, boolean setEndKeys) 
  throws IOException {
    createWebPageData(store);
    
    //create sorted set of urls
    List<String> sortedUrls = new ArrayList<String>(); 
    for(String url: URLS) {
      sortedUrls.add(url);
    }
    Collections.sort(sortedUrls);
    
    //try all ranges
    for(int i=0; i<sortedUrls.size(); i++) {
      for(int j=i; j<sortedUrls.size(); j++) {
        Query<String, WebPage> query = store.newQuery();
        if(setStartKeys)
          query.setStartKey(sortedUrls.get(i));
        if(setEndKeys)
          query.setEndKey(sortedUrls.get(j));
        Result<String, WebPage> result = query.execute();
        
        int r=0;
        while(result.next()) {
          WebPage page = result.get();
          assertWebPage(page, URL_INDEXES.get(page.getUrl().toString()));
          r++;
        }
        
        int expectedLength = (setEndKeys ? j+1: sortedUrls.size()) - 
                             (setStartKeys ? i: 0);
        Assert.assertEquals(expectedLength, r);
        if(!setEndKeys)
          break;
      }
      if(!setStartKeys)
        break;
    }
  }
  
  public static void testQueryWebPages(DataStore<String, WebPage> store) 
  throws IOException {
    testQueryWebPageKeyRange(store, false, false);
  }
  
  public static void testQueryWebPageStartKey(DataStore<String, WebPage> store) 
  throws IOException {
    testQueryWebPageKeyRange(store, true, false);
  }
  
  public static void testQueryWebPageEndKey(DataStore<String, WebPage> store) 
  throws IOException {
    testQueryWebPageKeyRange(store, false, true);
  }
  
  public static void testQueryWebPageKeyRange(DataStore<String, WebPage> store) 
  throws IOException {
    testQueryWebPageKeyRange(store, true, true);
  }
  
  public static void testQueryWebPageQueryEmptyResults(DataStore<String, WebPage> store) 
    throws IOException {
    createWebPageData(store);
    
    //query empty results
    Query<String, WebPage> query = store.newQuery();
    query.setStartKey("aa");
    query.setEndKey("ab");
    
    Result<String, WebPage> result = query.execute();
    int numResults = 0;
    while(result.next()) {
      numResults++;
    }
    Assert.assertEquals(0, numResults);
    
    //query empty results for one key
    query = store.newQuery();
    query.setKey("aa");
    
    result = query.execute();
    numResults = 0;
    while(result.next()) {
      numResults++;
    }
    Assert.assertEquals(0, numResults);
  }
  
  public static void testGetPartitions(DataStore<String, WebPage> store) 
  throws IOException {
    createWebPageData(store);
    testGetPartitions(store, store.newQuery());
  }
  
  public static void testGetPartitions(DataStore<String, WebPage> store
      , Query<String, WebPage> query) throws IOException {
    List<PartitionQuery<String, WebPage>> partitions = store.getPartitions(query);
    
    Assert.assertNotNull(partitions);
    Assert.assertTrue(partitions.size() > 0);
    
    for(PartitionQuery<String, WebPage> partition:partitions) {
      Assert.assertNotNull(partition);
    }
    
    assertPartitions(store, query, partitions);
  }
  
  public static void assertPartitions(DataStore<String, WebPage> store, 
      Query<String, WebPage> query, List<PartitionQuery<String,WebPage>> partitions) 
  throws IOException {
    
    int count = 0, partitionsCount = 0;
    Map<String, Integer> results = new HashMap<String, Integer>();
    Map<String, Integer> partitionResults = new HashMap<String, Integer>();
    
    //execute query and count results
    Result<String, WebPage> result = store.execute(query);
    Assert.assertNotNull(result);
    
    while(result.next()) {
      Assert.assertNotNull(result.getKey());
      Assert.assertNotNull(result.get());
      results.put(result.getKey(), result.get().hashCode()); //keys are not reused, so this is safe
      count++;
    }
    result.close();
    
    Assert.assertTrue(count > 0); //assert that results is not empty
    Assert.assertEquals(count, results.size()); //assert that keys are unique
    
    for(PartitionQuery<String, WebPage> partition:partitions) {
      Assert.assertNotNull(partition);
      
      result = store.execute(partition);
      Assert.assertNotNull(result);
      
      while(result.next()) {
        Assert.assertNotNull(result.getKey());
        Assert.assertNotNull(result.get());
        partitionResults.put(result.getKey(), result.get().hashCode());
        partitionsCount++;
      }
      result.close();
      
      Assert.assertEquals(partitionsCount, partitionResults.size()); //assert that keys are unique
    }
    
    Assert.assertTrue(partitionsCount > 0);
    Assert.assertEquals(count, partitionsCount);
    
    for(Map.Entry<String, Integer> r : results.entrySet()) {
      Integer p = partitionResults.get(r.getKey());
      Assert.assertNotNull(p);
      Assert.assertEquals(r.getValue(), p);
    }
  }
}
