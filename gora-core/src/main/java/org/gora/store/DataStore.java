package org.gora.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;

public interface DataStore<K, T extends Persistent> extends Closeable {

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
  
  public abstract void createTable() throws IOException;

  public abstract T newInstance() throws IOException;
  
  public abstract T get(K key, String[] fields) throws IOException;
  
  public abstract void put(K key, T obj) throws IOException;

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
  
  public abstract void sync() throws IOException;
  
  @Override
  public abstract void close() throws IOException;
}
