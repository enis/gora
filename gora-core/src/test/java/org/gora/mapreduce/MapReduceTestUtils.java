
package org.gora.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.gora.examples.WebPageDataCreator;
import org.gora.examples.generated.TokenDatum;
import org.gora.examples.generated.WebPage;
import org.gora.examples.mapreduce.QueryCounter;
import org.gora.examples.mapreduce.WordCount;
import org.gora.query.Query;
import org.gora.store.DataStore;
import org.junit.Assert;

public class MapReduceTestUtils {

  private static final Log log = LogFactory.getLog(MapReduceTestUtils.class);
  
  /** Tests by running the {@link QueryCounter} mapreduce job */
  public static void testCountQuery(DataStore<String, WebPage> dataStore
      , Configuration conf) 
  throws Exception {
    
    dataStore.setConf(conf);
    
    //create input
    WebPageDataCreator.createWebPageData(dataStore);
    
    
    QueryCounter<String,WebPage> counter = new QueryCounter<String,WebPage>(conf);
    Query<String,WebPage> query = dataStore.newQuery();
    query.setFields(WebPage._ALL_FIELDS);
    
    dataStore.close();
    
    
    //run the job
    log.info("running count query job");
    long result = counter.countQuery(dataStore, query);
    log.info("finished count query job");
    
    //assert results
    Assert.assertEquals(WebPageDataCreator.URLS.length, result);
    
  }
 
  public static void testWordCount(Configuration conf, 
      DataStore<String,WebPage> inStore, DataStore<String, 
      TokenDatum> outStore) throws Exception {
    inStore.setConf(conf);
    outStore.setConf(conf);
    
    //create input
    WebPageDataCreator.createWebPageData(inStore);
    
    //run the job
    WordCount wordCount = new WordCount(conf);
    wordCount.wordCount(inStore, outStore);
    
    //assert results
    HashMap<String, Integer> actualCounts = new HashMap<String, Integer>();
    for(String content : WebPageDataCreator.CONTENTS) {
      for(String token:content.split(" ")) {
        Integer count = actualCounts.get(token);
        if(count == null) 
          count = 0;
        actualCounts.put(token, ++count);
      }
    }
    for(Map.Entry<String, Integer> entry:actualCounts.entrySet()) {
      assertTokenCount(outStore, entry.getKey(), entry.getValue()); 
    }
  }
  
  private static void assertTokenCount(DataStore<String, TokenDatum> outStore,
      String token, int count) throws IOException {
    TokenDatum datum = outStore.get(token, null);
    Assert.assertNotNull("token:" + token + " cannot be found in datastore", datum);
    Assert.assertEquals("count for token:" + token + " is wrong", count, datum.getCount());
  }
}
