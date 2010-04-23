
package org.gora.store;

import junit.framework.Assert;

import org.gora.mock.persistency.MockPersistent;
import org.gora.mock.store.MockDataStore;
import org.junit.Before;
import org.junit.Test;

public class TestDataStoreFactory {
  
  @Before
  public void setUp() {
  }

  @Test
  public void testGetDataStore() throws ClassNotFoundException {
    DataStore<?,?> dataStore = DataStoreFactory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    Assert.assertNotNull(dataStore);
  }
  
  @Test
  public void testReadProperties() {
    //indirect testing
    DataStore<?,?> dataStore = DataStoreFactory.getDataStore(String.class, MockPersistent.class);
    Assert.assertNotNull(dataStore);
    Assert.assertEquals(MockDataStore.class, dataStore.getClass());
  }
  
  @Test
  public void testGetDataStore2() throws ClassNotFoundException {
    DataStore<?,?> dataStore1 = DataStoreFactory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    DataStore<?,?> dataStore2 = DataStoreFactory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    DataStore<?,?> dataStore3 = DataStoreFactory.getDataStore("org.gora.mock.store.MockDataStore"
        , String.class, MockPersistent.class);
    
    Assert.assertTrue(dataStore1 == dataStore2);
    Assert.assertNotSame(dataStore1, dataStore3);
  }
  
}
