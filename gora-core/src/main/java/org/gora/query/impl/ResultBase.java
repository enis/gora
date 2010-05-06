
package org.gora.query.impl;

import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;

/**
 * Base class for {@link Result} implementations.
 */
public abstract class ResultBase<K, T extends Persistent> 
  implements Result<K, T> {

  protected final DataStore<K,T> dataStore;
  
  protected final Query<K, T> query;
  
  protected K key;
  
  protected T persistent;
  
  /** Query limit */
  protected long limit;
  
  /** How far we have proceeded*/
  protected long offset = 0;
  
  public ResultBase(DataStore<K,T> dataStore, Query<K,T> query) {
    this.dataStore = dataStore;
    this.query = query;
    this.limit = query.getLimit();
  }
  
  @Override
  public DataStore<K, T> getDataStore() {
    return dataStore;
  }
  
  @Override
  public Query<K, T> getQuery() {
    return query;
  }
  
  @Override
  public T get() {
    return persistent;
  }
  
  @Override
  public K getKey() {
    return key;
  }
    
  @Override
  public Class<K> getKeyClass() {
    return getDataStore().getKeyClass();
  }
  
  @Override
  public Class<T> getPersistentClass() {
    return getDataStore().getPersistentClass();
  }
  
  /**
   * Returns whether the limit for the query is reached. 
   */
  protected boolean isLimitReached() {
    if(limit > 0 && offset++ > limit) {
      return true;
    }
    return false;
  }
  
}
