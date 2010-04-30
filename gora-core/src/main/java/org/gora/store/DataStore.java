package org.gora.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;

/**
 * DataStore handles actual object persistence. Objects can be persisted, 
 * fetched, queried or deleted by the DataStore methods. DataStores can be
 * constructed by an instance of {@link DataStoreFactory}.   
 * 
 * <p> DataStores implementations should be thread safe.
 * @param <K> the class of keys in the datastore
 * @param <T> the class of persistent objects in the datastore
 */
public interface DataStore<K, T extends Persistent> extends Closeable {

  /**
   * Initializes this DataStore.
   * @param keyClass the class of the keys
   * @param persistentClass the class of the persistent objects
   * @param properties extra metadata  
   * @throws IOException 
   */
  public abstract void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException;
  
  /**
   * Sets the class of the keys
   * @param keyClass the class of keys
   */
  public abstract void setKeyClass(Class<K> keyClass);
  
  /**
   * Returns the class of the keys
   * @return class of the keys
   */
  public abstract Class<K> getKeyClass();
  
  /**
   * Sets the class of the persistent objects
   * @param persistentClass class of persistent objects
   */
  public abstract void setPersistentClass(Class<T> persistentClass);
  
  /**
   * Returns the class of the persistent objects
   * @return class of the persistent objects
   */
  public abstract Class<T> getPersistentClass();
  
  /**
   * Creates the optional schema or table (or similar) in the datastore 
   * to hold the objects. If the schema is already created previously, 
   * or the underlying data model does not support 
   * or need this operation, the operation is ignored. 
   */
  public abstract void createSchema() throws IOException;

  /**
   * Returns a new instance of the managed persistent object.
   * @return a new instance of the managed persistent object.
   */
  public abstract T newInstance() throws IOException;

  /**
   * Returns the 
   * @param key
   * @param fields
   * @return
   * @throws IOException
   */
  public abstract T get(K key, String[] fields) throws IOException;
  
  /**
   * Inserts the persistent object with the given key.
   */
  public abstract void put(K key, T obj) throws IOException;

  /**
   * Deletes the object with the given key
   * @param key the key of the object
   * @throws IOException
   */
  public abstract void delete(K key) throws IOException;

  /**
   * Executes the given query and returns the results.
   * @param query the query to execute.
   * @return the results as a {@link Result} object.
   */
  public abstract Result<K,T> execute(Query<K, T> query) throws IOException;

  /**
   * Constructs and returns a new Query.
   * @return a new Query.
   */
  public abstract Query<K, T> newQuery();
  
  /**
   * Partitions the given query and returns a list of {@link PartitionQuery}s, 
   * which will execute on local data.
   * @param query the base query to create the partitions for. If the query 
   * is null, then the data store returns the partitions for the default query
   * (returning every object) 
   * @return a List of PartitionQuery's 
   */
  public abstract List<PartitionQuery<K,T>> getPartitions(Query<K,T> query) 
    throws IOException;
  
  /**
   * Forces the write caches to be flushed.
   */
  public abstract void flush() throws IOException;
  
  @Override
  public abstract void close() throws IOException;
}
