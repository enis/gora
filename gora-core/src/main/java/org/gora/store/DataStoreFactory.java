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
  
  private static String propertiesFile = GORA_DEFAULT_PROPERTIES_FILE; 
  
  private static String defaultDataStoreClass;
  
  private static HashMap<Integer, DataStore<?,?>> dataStores;
  
  private static Properties properties;
  
  static {
    dataStores = new HashMap<Integer, DataStore<?,?>>();
    try {
      readProperties();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  private DataStoreFactory() { }
  
  private static <K, T extends Persistent> void initializeDataStore(
      DataStore<K, T> dataStore, Class<K> keyClass, Class<T> persistent
      , Properties properties) {
    dataStore.initialize(keyClass, persistent, properties);
  }
  
  private static <D extends DataStore<K,T>, K, T extends Persistent> 
  D createDataStore(Class<D> dataStoreClass
      , Class<K> keyClass, Class<T> persistent, Properties properties) {
    try {
      D dataStore = 
        ReflectionUtils.newInstance(dataStoreClass);
      initializeDataStore(dataStore, keyClass, persistent, properties);
      return dataStore;
      
    } catch (Exception ex) {
      log.error(StringUtils.stringifyException(ex));
      return null;
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <D extends DataStore<K,T>, K, T extends Persistent> 
  D getDataStore( Class<D> dataStoreClass, Class<K> keyClass, 
      Class<T> persistentClass) {
    int hash = getDataStoreKey(dataStoreClass, keyClass, persistentClass);
    
    D dataStore = (D) dataStores.get(hash);
    if(dataStore == null) {
      dataStore = createDataStore(dataStoreClass, keyClass, persistentClass
          , properties);
      dataStores.put(hash, dataStore);
    }
    return dataStore;  
  }
  
  @SuppressWarnings("unchecked")
  public static synchronized <K, T extends Persistent> DataStore<K, T> getDataStore(
      String dataStoreClass, Class<K> keyClass, Class<T> persistentClass) 
      throws ClassNotFoundException {
    
    Class<? extends DataStore<K,T>> c 
        = (Class<? extends DataStore<K, T>>) Class.forName(dataStoreClass);
    return getDataStore(c, keyClass, persistentClass); 
  }
  
  @SuppressWarnings("unchecked")
  public static synchronized DataStore getDataStore(
      String dataStoreClass, String keyClass, String persistentClass) 
    throws ClassNotFoundException {
    
    Class k = Class.forName(keyClass);
    Class p = Class.forName(persistentClass);
    return getDataStore(dataStoreClass, k, p);
  }
  
  public static <K, T extends Persistent> DataStore<K, T> getDataStore(
      Class<K> keyClass, Class<T> persistent) {
    try {
      return getDataStore(defaultDataStoreClass, keyClass, persistent);
    } catch (ClassNotFoundException ex) {
      return null;
    }
  }
  
  private static int getDataStoreKey(
      Class<?> dataStoreClass, Class<?> keyClass, Class<?> persistent) {
    
    long hash = (((dataStoreClass.hashCode() * 27L)
        + keyClass.hashCode()) * 31) + persistent.hashCode();   
    
    return (int)hash;
  }
  
  private static Properties readProperties() throws IOException {
    Properties properties = new Properties();
    if(propertiesFile != null) {
      InputStream stream = DataStoreFactory.class.getClassLoader()
        .getResourceAsStream(propertiesFile);
      if(stream != null) {
        try {
          properties.load(stream);
          setProperties(properties);
          return properties;
        } finally {
          stream.close();  
        }
      }
    }
    log.warn("Gora properties are not loaded!");
    return null;
  }
  
  private static void setProperties(Properties properties) {
    defaultDataStoreClass = properties.getProperty(GORA_DEFAULT_DATASTORE_KEY);
    DataStoreFactory.properties = properties;
  }
  
}