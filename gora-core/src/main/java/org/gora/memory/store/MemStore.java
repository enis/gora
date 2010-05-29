
package org.gora.memory.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.gora.persistency.Persistent;
import org.gora.persistency.impl.StateManagerImpl;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.query.impl.PartitionQueryImpl;
import org.gora.query.impl.QueryBase;
import org.gora.query.impl.ResultBase;
import org.gora.store.DataStore;
import org.gora.store.impl.DataStoreBase;

/**
 * Memory based {@link DataStore} implementation for tests.
 */
public class MemStore<K, T extends Persistent> extends DataStoreBase<K, T> {
 
  public static class MemQuery<K, T extends Persistent> extends QueryBase<K, T> {
    public MemQuery() {
      super(null);
    }
    public MemQuery(DataStore<K, T> dataStore) {
      super(dataStore);
    }
  }
  
  public static class MemResult<K, T extends Persistent> extends ResultBase<K, T> {
    private NavigableMap<K, T> map;
    private Iterator<K> iterator;
    public MemResult(DataStore<K, T> dataStore, Query<K, T> query
        , NavigableMap<K, T> map) {
      super(dataStore, query);
      this.map = map;
      iterator = map.navigableKeySet().iterator();
    }
    @Override
    public void close() throws IOException { }
    @Override
    public float getProgress() throws IOException {
      return 0;
    }
    
    @Override
    protected void clear() {  } //do not clear the object in the store 
    
    @Override
    public boolean nextInner() throws IOException {
      if(!iterator.hasNext()) {
        return false; 
      }
      
      key = iterator.next();
      persistent = map.get(key);
      
      return true;
    }
  }
  
  private TreeMap<K, T> map = new TreeMap<K, T>();  
  
  @Override
  public boolean delete(K key) throws IOException {
    return map.remove(key) != null;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) throws IOException {
    long deletedRows = 0;
    Result<K,T> result = query.execute();
    
    while(result.next()) {
      if(delete(result.getKey()))
        deletedRows++;
    }
    
    return 0;
  }
  
  @Override
  public Result<K, T> execute(Query<K, T> query) throws IOException {
    K startKey = query.getStartKey();
    K endKey = query.getEndKey();
    if(startKey == null) {
      startKey = map.firstKey();
    }
    if(endKey == null) {
      endKey = map.lastKey();
    }
    
    //check if query.fields is null
    query.setFields(getFieldsToQuery(query.getFields()));
    
    NavigableMap<K, T> submap = map.subMap(startKey, true, endKey, true);
    
    return new MemResult<K,T>(this, query, submap);
  }

  @Override
  public T get(K key, String[] fields) throws IOException {
    T obj = map.get(key);
    return getPersistent(obj, getFieldsToQuery(fields));
  }

  /**
   * Returns a clone with exactly the requested fields shallowly copied 
   */
  @SuppressWarnings("unchecked")
  private static<T extends Persistent> T getPersistent(T obj, String[] fields) {
    if(Arrays.equals(fields, obj.getFields())) {
      return obj;
    }
    T newObj = (T) obj.newInstance(new StateManagerImpl());
    for(String field:fields) {
      int index = newObj.getFieldIndex(field);
      newObj.put(index, obj.get(index));
    }
    return newObj;
  }
  
  @Override
  public Query<K, T> newQuery() {
    return new MemQuery<K, T>(this);
  }

  @Override
  public void put(K key, T obj) throws IOException {
    map.put(key, obj);
  }

  @Override
  /**
   * Returns a single partition containing the original query
   */
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    List<PartitionQuery<K, T>> list = new ArrayList<PartitionQuery<K,T>>();
    list.add(new PartitionQueryImpl<K, T>(query));
    return list;
  }
  
  @Override
  public void close() throws IOException {
    map.clear();
  }

  @Override
  public void createSchema() throws IOException { }
  
  @Override
  public void deleteSchema() throws IOException {
    map.clear();
  }
  
  @Override
  public boolean schemaExists() throws IOException {
    return true;
  }
  
  @Override
  public void flush() throws IOException { }
}
