package org.gora.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.gora.RowScanner;
import org.gora.TableRow;

public abstract class TableSerializer<K, R extends TableRow>
extends Configured implements Closeable {

  private Class<K> keyClass;
  private Class<R> rowClass;
  
  public TableSerializer(Configuration conf, Class<K> keyClass, Class<R> rowClass) {
    super(conf);
    this.keyClass = keyClass;
    this.rowClass = rowClass;
  }

  protected Class<K> getKeyClass() {
    return keyClass;
  }
  
  protected Class<R> getRowClass() {
    return rowClass;
  }
  
  public abstract void createTable() throws IOException;

  public abstract R makeRow() throws IOException;
  
  public abstract R readRow(K key, String[] fields) throws IOException;
  
  public abstract void updateRow(K key, R row) throws IOException;

  public abstract void sync() throws IOException;

  public abstract void deleteRow(K key) throws IOException;

  public abstract List<InputSplit> getSplits(K startRow, K stopRow, JobContext context)
  throws IOException;

  public abstract RowScanner<K, R> makeScanner(K startRow, K stopRow, String[] fields)
  throws IOException;

  public abstract RowScanner<K, R> makeScanner(InputSplit split, String[] fields)
  throws IOException;
}
