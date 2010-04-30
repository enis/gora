
package org.gora.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.gora.example.WebPageDataCreator;
import org.gora.example.generated.WebPage;
import org.gora.example.mapreduce.QueryCounter;
import org.gora.query.Query;
import org.gora.store.DataStore;
import org.junit.Assert;

public class MapReduceTestUtils {

  /** Tests by running the {@link QueryCounter} mapreduce job */
  public static void testCountQuery(DataStore<String, WebPage> dataStore
      , Configuration conf) 
  throws Exception {
    WebPageDataCreator.createWebPageData(dataStore);
    QueryCounter<String,WebPage> counter = new QueryCounter<String,WebPage>(conf);
    Query<String,WebPage> query = dataStore.newQuery();
    query.setFields(WebPage._ALL_FIELDS);
    System.out.println("query:" + query);
    long result = counter.countQuery(dataStore, query);
    
    Assert.assertEquals(WebPageDataCreator.URLS.length, result);
  }
  
}
