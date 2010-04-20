
package org.gora.store;

import junit.framework.Assert;

import org.gora.mock.persistency.MockPersistent;
import org.gora.mock.store.MockDataStore;
import org.junit.Before;
import org.junit.Test;

public class TestDataStoreFactory {

  private DataStoreFactory factory;
  
  @Before
  public void setUp() {
    factory = new DataStoreFactory();
  }

  @Test
  public void testGetDataStore() {
    DataStore<?,?> dataStore = factory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    Assert.assertNotNull(dataStore);
  }
  
  public void testReadProperties() {
    //indirect testing
    DataStore<?,?> dataStore = factory.getDataStore(Object.class, MockPersistent.class);
    Assert.assertNotNull(dataStore);
    Assert.assertEquals(MockDataStore.class, dataStore.getClass());
  }
  
  public void testGetDataStore2() {
    DataStore<?,?> dataStore1 = factory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    DataStore<?,?> dataStore2 = factory.getDataStore("org.gora.mock.store.MockDataStore"
        , Object.class, MockPersistent.class);
    DataStore<?,?> dataStore3 = factory.getDataStore("org.gora.mock.store.MockDataStore"
        , String.class, MockPersistent.class);
    
    Assert.assertEquals(dataStore1, dataStore2);
    Assert.assertNotSame(dataStore1, dataStore3);
  }
  
}
