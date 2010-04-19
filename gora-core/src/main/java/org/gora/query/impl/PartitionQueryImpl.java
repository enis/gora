
package org.gora.query.impl;

import org.gora.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.store.DataStore;

/**
 * Implementation for {@link PartitionQuery}.
 */
public class PartitionQueryImpl<K, T extends Persistent> 
  extends QueryBase<K, T> implements PartitionQuery<K, T> {

  protected final Query<K, T> baseQuery;
  protected final String[] locations;
  
  public PartitionQueryImpl(Query<K, T> baseQuery, String... locations) {
    this(baseQuery, null, null, locations);
  }

  public PartitionQueryImpl(Query<K, T> baseQuery, K startKey, K endKey, 
      String... locations) {
    super(baseQuery.getDataStore());
    this.baseQuery = baseQuery;
    this.locations = locations;
    setStartKey(startKey);
    setEndKey(endKey);
  }
  
  public String[] getLocations() {
    return locations;
  }
  
  /* Override everything except start-key/end-key */
  
  @Override
  public String[] getFields() {
    return baseQuery.getFields();
  }
  
  @Override
  public DataStore<K, T> getDataStore() {
    return baseQuery.getDataStore();
  }
  
  @Override
  public long getTimestamp() {
    return baseQuery.getTimestamp();
  }
  
  @Override
  public long getStartTime() {
    return baseQuery.getStartTime();
  }
  
  @Override
  public long getEndTime() {
    return baseQuery.getEndTime();
  }
  
  @Override
  public Long getLimit() {
    return baseQuery.getLimit();
  }
  
  @Override
  public void setFields(String... fields) {
    baseQuery.setFields(fields);
  }
  
  @Override
  public void setTimestamp(long timestamp) {
    baseQuery.setTimestamp(timestamp);
  }
  
  @Override
  public void setStartTime(long startTime) {
    baseQuery.setStartTime(startTime);
  }
  
  @Override
  public void setEndTime(long endTime) {
    baseQuery.setEndTime(endTime);
  }
  
  @Override
  public void setTimeRange(long startTime, long endTime) {
    baseQuery.setTimeRange(startTime, endTime);
  }
  
  @Override
  public void setLimit(long limit) {
    baseQuery.setLimit(limit);
  }
  
}
