
package org.gora.query.impl;

import java.io.IOException;

import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;

/**
 * Base class for Query implementations. 
 */
public abstract class QueryBase<K, T extends Persistent> 
implements Query<K,T> {

  protected DataStore<K,T> dataStore;
  
  protected String queryString;
  protected String[] fields;
  
  protected K startKey;
  protected K endKey;
  
  protected Long startTime;
  protected Long endTime;
  
  protected String filter;
  
  protected Long limit;
  
  protected boolean isCompiled = false;

  public QueryBase(DataStore<K,T> dataStore) {
    this.dataStore = dataStore;
  } 

  @Override
  public Result<K,T> execute() throws IOException {
    //compile();
    return dataStore.execute(this);
  }

//  @Override
//  public void compile() {
//    if(!isCompiled) {
//      isCompiled = true;
//    }
//  }
  
  @Override
  public DataStore<K, T> getDataStore() {
    return dataStore;
  }

//  @Override
//  public void setQueryString(String queryString) {
//    this.queryString = queryString;
//  }
//  
//  @Override
//  public String getQueryString() {
//    return queryString;
//  }

  @Override
  public void setFields(String... fields) {
    this.fields = fields;
  }

  public String[] getFields() {
    return fields;
  }
  
  @Override
  public void setKey(K key) {
    setKeyRange(key, key);
  }

  @Override
  public void setStartKey(K startKey) {
    this.startKey = startKey;
  }

  @Override
  public void setEndKey(K endKey) {
    this.endKey = endKey;
  }

  @Override
  public void setKeyRange(K startKey, K endKey) {
    this.startKey = startKey;
    this.endKey = endKey;
  }

  @Override
  public K getKey() {
    if(startKey == endKey) {
      return startKey; //address comparison
    }
    return null;
  }
  
  @Override
  public K getStartKey() {
    return startKey;
  }
  
  @Override
  public K getEndKey() {
    return endKey;
  }
  
  @Override
  public void setTimestamp(long timestamp) {
    setTimeRange(timestamp, timestamp);
  }
  
  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
  
  @Override
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }
  
  @Override
  public void setTimeRange(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
  }
  
  @Override
  public long getTimestamp() {
    if(startTime != null && endTime != null) {
      if(startTime.longValue() == endTime.longValue()) {
        return startTime;
      }
    }
    return -1;
  }
  
  @Override
  public long getStartTime() {
    return startTime == null ? -1 : startTime;
  }
  
  @Override
  public long getEndTime() {
    return endTime == null ? -1 : endTime;
  }
  
//  @Override
//  public void setFilter(String filter) {
//    this.filter = filter;
//  }
//  
//  @Override
//  public String getFilter() {
//    return filter;
//  }
  
  @Override
  public void setLimit(long limit) {
    this.limit = limit;
  }
  
  @Override
  public Long getLimit() {
    return limit;
  }
  
}
