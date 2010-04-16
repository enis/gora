package org.gora.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.gora.Persistent;
import org.gora.RowScanner;

public interface DataStore<K, T extends Persistent> extends Closeable {

  public abstract void setKeyClass(Class<K> keyClass);
  
  public abstract Class<K> getKeyClass();
  
  public abstract void setPersistentClass(Class<T> persistentClass);
  
  public abstract Class<T> getPersistentClass();
  
  public abstract void createTable() throws IOException;

  public abstract T newInstance() throws IOException;
  
  public abstract T get(K key, String[] fields) throws IOException;
  
  public abstract void put(K key, T obj) throws IOException;

  public abstract void delete(T obj) throws IOException;
  
  public abstract void deleteByKey(K key) throws IOException;

  public abstract void sync() throws IOException;
  
  public abstract List<InputSplit> getSplits(K startRow, K stopRow, JobContext context)
  throws IOException;

  public abstract RowScanner<K, T> makeScanner(K startRow, K stopRow, String[] fields)
  throws IOException;

  public abstract RowScanner<K, T> makeScanner(InputSplit split, String[] fields)
  throws IOException;
  
  @Override
  public void close() throws IOException;
}
