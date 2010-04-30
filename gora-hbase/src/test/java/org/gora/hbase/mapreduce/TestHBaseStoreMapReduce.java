
package org.gora.hbase.mapreduce;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.gora.example.generated.WebPage;
import org.gora.hbase.store.HBaseStore;
import org.gora.mapreduce.MapReduceTestUtils;
import org.gora.store.DataStoreFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests related to {@link HBaseStore} using mapreduce.
 */
public class TestHBaseStoreMapReduce extends HBaseClusterTestCase{

  private HBaseStore<String, WebPage> webPageStore;
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
   webPageStore = (HBaseStore<String, WebPage>) DataStoreFactory.getDataStore(
        HBaseStore.class, String.class, WebPage.class);
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    webPageStore.close();
  }
  
  @Test
  public void testCountQuery() throws Exception {
    MapReduceTestUtils.testCountQuery(webPageStore, conf);
  }
  
}
