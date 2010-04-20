package org.gora.store;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.gora.persistency.Persistent;
import org.gora.util.ReflectionUtils;

/**
 * A Factory for {@link DataStore}s. DataStoreFactory instances are thread-safe.
 */
public class DataStoreFactory {
  
  public static final Log log = LogFactory.getLog(DataStoreFactory.class);
  
  public static final String GORA_DEFAULT_PROPERTIES_FILE = "gora.properties";
  
  public static final String GORA_DEFAULT_DATASTORE_KEY = "gora.datastore.default";
  
  private String propertiesFile = GORA_DEFAULT_PROPERTIES_FILE; 
  
  private String defaultDataStoreClass;
  
  private HashMap<Integer, DataStore<?,?>> dataStores;
  
  public DataStoreFactory() {
    dataStores = new HashMap<Integer, DataStore<?,?>>();
    try {
      readProperties();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  public DataStoreFactory(String propertiesFile) {
    this();
    this.propertiesFile = propertiesFile;
  }

  private <K, T extends Persistent> void initializeDataStore(
      DataStore<K, T> dataStore, Class<K> keyClass, Class<T> persistent) {
    dataStore.setKeyClass(keyClass);
    dataStore.setPersistentClass(persistent);
  }
  
  @SuppressWarnings("unchecked")
  private <K, T extends Persistent> DataStore<K,T> createDataStore(
      String dataStoreClass, Class<K> keyClass, Class<T> persistent) {
    try {
      DataStore<K, T> dataStore = 
        (DataStore<K, T>) ReflectionUtils.newInstance(dataStoreClass);
      initializeDataStore(dataStore, keyClass, persistent);
      return dataStore;
      
    } catch (Exception ex) {
      log.error(StringUtils.stringifyException(ex));
      return null;
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized <K, T extends Persistent> DataStore<K, T> getDataStore(
      String dataStoreClass, Class<K> keyClass, Class<T> persistentClass) {
    int hash = getDataStoreKey(dataStoreClass, keyClass, persistentClass);
    
    DataStore dataStore = dataStores.get(hash);
    if(dataStore == null) {
      dataStore = createDataStore(dataStoreClass, keyClass, persistentClass);
    }
    return dataStore;
  }
  
  public <K, T extends Persistent> DataStore<K, T> getDataStore(
      Class<K> keyClass, Class<T> persistent) {
    return getDataStore(defaultDataStoreClass, keyClass, persistent);
  }
  
  private <K, T extends Persistent> int getDataStoreKey(
      String dataStoreClass, Class<K> keyClass, Class<T> persistent) {
    
    long hash = (((dataStoreClass.hashCode() * 27L) 
        + keyClass.hashCode()) * 31) + persistent.hashCode();   
    
    return (int)hash;
  }
  
  private void readProperties() throws IOException {
    Properties properties = new Properties();
    if(propertiesFile != null) {
      InputStream stream = this.getClass().getClassLoader()
        .getResourceAsStream(propertiesFile);
      if(stream != null) {
        try {
          properties.load(stream);
          setProperties(properties);
          return;
        } finally {
          stream.close();  
        }
      }
    }
    log.warn("Gora properties are not loaded!");
  }
  
  private void setProperties(Properties properties) {
    defaultDataStoreClass = properties.getProperty(GORA_DEFAULT_DATASTORE_KEY);
  }
  
}