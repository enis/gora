package org.gora.store;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.gora.persistency.Persistent;
import org.gora.store.impl.DataStoreBase;
import org.gora.util.ReflectionUtils;

/**
 * A Factory for {@link DataStore}s. DataStoreFactory instances are thread-safe.
 */
public class DataStoreFactory {
  
  public static final Log log = LogFactory.getLog(DataStoreFactory.class);
  
  public static final String GORA_DEFAULT_PROPERTIES_FILE = "gora.properties";
  
  public static final String GORA_DEFAULT_DATASTORE_KEY = "gora.datastore.default";

  public static final String GORA = "gora";
  
  public static final String DATASTORE = "datastore";
  
  public static final String AUTO_CREATE_SCHEMA = "autocreateschema";
  
  public static final String INPUT_PATH  = "input.path";
  
  public static final String OUTPUT_PATH = "output.path";
  
  public static final String MAPPING_FILE = "mapping.file";
  
  private static String propertiesFile = GORA_DEFAULT_PROPERTIES_FILE; 
  
  private static String defaultDataStoreClass;
  
  private static HashMap<Integer, DataStore<?,?>> dataStores;
  
  public static Properties properties;
  
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
      , Properties properties) throws IOException {
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
  
  private static String getClassname(Class<?> clazz) {
    String classname = clazz.getName();
    String[] parts = classname.split("\\.");
    return parts[parts.length-1];
  }
  
  /**
   * Tries to find a property with the given baseKey. First the property 
   * key constructed as "gora.&lt;classname&gt;.&lt;baseKey&gt;" is searched. 
   * If not found, the property keys for all superclasses is recursively 
   * tested. Lastly, the property key constructed as 
   * "gora.datastore.&lt;baseKey&gt;" is searched.  
   * @return the first found value, or defaultValue
   */
  public static String findProperty(Properties properties
      , DataStore<?, ?> store, String baseKey, String defaultValue) {
    
    //recursively try the class names until the base class
    Class<?> clazz = store.getClass();
    while(true) {
      String fullKey = GORA + "." + getClassname(clazz) + "." + baseKey;
      String value = getProperty(properties, fullKey);
      if(value != null) {
        return value;
      }
      //try once with lowercase
      value = getProperty(properties, fullKey.toLowerCase());
      if(value != null) {
        return value;
      }
      
      if(clazz.equals(DataStoreBase.class)) {
        break;
      }
      clazz = clazz.getSuperclass();
      if(clazz == null) {
        break;
      }
    }
    //try with "datastore"
    String fullKey = GORA + "." + DATASTORE + "." + baseKey;
    String value = getProperty(properties, fullKey);
    if(value != null) {
      return value;
    }
    return defaultValue;
  }
  
  public static boolean findBooleanProperty(Properties properties
      , DataStore<?, ?> store, String baseKey, String defaultValue) {
    return Boolean.parseBoolean(findProperty(properties, store, baseKey, defaultValue));
  }
  
  public static boolean getAutoCreateSchema(Properties properties
      , DataStore<?,?> store) {
    return findBooleanProperty(properties, store, AUTO_CREATE_SCHEMA, "true");
  }
  
  /**
   * Returns the input path as read from the properties for file-backed data stores.
   */
  public static String getInputPath(Properties properties, DataStore<?,?> store) {
    return findProperty(properties, store, INPUT_PATH, null);
  }
  
  /**
   * Returns the output path as read from the properties for file-backed data stores.
   */
  public static String getOutputPath(Properties properties, DataStore<?,?> store) {
    return findProperty(properties, store, OUTPUT_PATH, null);
  }
  
  public static String getMappingFile(Properties properties, DataStore<?,?> store
      , String defaultValue) {
    return findProperty(properties, store, MAPPING_FILE, defaultValue);
  }
  
  private static void setProperties(Properties properties) {
    defaultDataStoreClass = getProperty(properties, GORA_DEFAULT_DATASTORE_KEY);
    DataStoreFactory.properties = properties;
  }

  private static String getProperty(Properties properties, String key) {
    return getProperty(properties, key, null);
  }

  private static String getProperty(Properties properties, String key, String defaultValue) {
    if (properties == null) {
      return defaultValue;
    }
    String result = properties.getProperty(key);
    if (result == null) {
      return defaultValue;
    }
    return result;
  }
}