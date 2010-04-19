
package org.gora.hbase.query;

import org.gora.hbase.store.HBaseStore;
import org.gora.persistency.Persistent;
import org.gora.query.impl.ResultBase;

public abstract class HBaseResult<K, T extends Persistent> 
  extends ResultBase<K, T> {

  public HBaseResult(HBaseStore<K,T> dataStore, HBaseQuery<K, T> query) {
    super(dataStore, query);
  }
  
  @Override
  public HBaseStore<K, T> getDataStore() {
    return (HBaseStore<K, T>) super.getDataStore();
  }
  
  @Override
  public HBaseQuery<K, T> getQuery() {
    return (HBaseQuery<K, T>) super.getQuery();
  }
}
