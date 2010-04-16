package org.gora.store;

import org.apache.hadoop.util.StringUtils;
import org.gora.Persistent;
import org.gora.util.ReflectionUtils;
import org.mortbay.log.Log;

/**
 * A Factory for {@link DataStore}s. 
 */
public class DataStoreFactory {
  
  public static final String GORA_DEFAULT_PROPERTIES_FILE = "gora.properties";
  
  public static final String GORA_DEFAULT_DATASTORE = "org.gora.store.MockDataStore";
  
  private String propertiesFile = GORA_DEFAULT_PROPERTIES_FILE; 
  
  public DataStoreFactory() {
  }
  
  public DataStoreFactory(String propertiesFile) {
    this.propertiesFile = propertiesFile;
  }
  
  @SuppressWarnings("unchecked")
  public <K, T extends Persistent> DataStore<K, T> getDataStore(String dataStoreClass
      , Class<K> keyClass, Class<T> persistent) {
    try {
      DataStore<K, T> dataStore = (DataStore<K, T>) ReflectionUtils.newInstance(dataStoreClass);
      initializeDataStore(dataStore, keyClass, persistent);
      return dataStore;
      
    } catch (Exception ex) {
      Log.warn(StringUtils.stringifyException(ex));
      return null;
    }
  }
  
  private <K, T extends Persistent> void initializeDataStore(DataStore<K, T> dataStore
      ,Class<K> keyClass, Class<T> persistent) {
    dataStore.setKeyClass(keyClass);
    dataStore.setPersistentClass(persistent);
  }
  
  public <K, T extends Persistent> DataStore<K, T> getDataStore(Class<T> persistent) {
    throw new RuntimeException("Not yet impl.");
  }
  
  public <K, T extends Persistent> DataStore<K, T> getDataStore(Class<K> keyClass
      , Class<T> persistent) {
    throw new RuntimeException("Not yet impl.");
  }
  
  /** Returns the default DataStore */
  public <K, T extends Persistent> DataStore<K, T> getDataStore() {
    throw new RuntimeException("Not yet impl.");
  }
  
}