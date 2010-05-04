package org.gora.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gora.persistency.Persistent;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;

public class GoraOutputFormat<K, T extends Persistent>
extends OutputFormat<K, T>{

  public static final String DATA_STORE_CLASS = "gora.outputformat.datastore.class";
  
  public static final String REDUCE_KEY_CLASS   = "gora.outputformat.reduce.key.class";

  public static final String REDUCE_VALUE_CLASS = "gora.outputformat.reduce.value.class";

  @Override
  public void checkOutputSpecs(JobContext context)
  throws IOException, InterruptedException { }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
  throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordWriter<K, T> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Class<? extends DataStore<K,T>> dataStoreClass 
      = (Class<? extends DataStore<K,T>>) conf.getClass(DATA_STORE_CLASS, null);
    Class<K> keyClass = (Class<K>) conf.getClass(REDUCE_KEY_CLASS, null);
    Class<T> rowClass = (Class<T>) conf.getClass(REDUCE_VALUE_CLASS, null);
    final DataStore<K, T> store =
      DataStoreFactory.getDataStore(dataStoreClass, keyClass, rowClass);
    
    return new RecordWriter<K, T>() {
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        store.flush();
      }

      @Override
      public void write(K key, T value)
      throws IOException, InterruptedException {
        store.put(key, value);
      }
    };
  }

}
