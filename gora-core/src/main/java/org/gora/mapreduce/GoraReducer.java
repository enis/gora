package org.gora.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.gora.persistency.Persistent;
import org.gora.store.DataStore;

/**
 * Optional base class for gora based {@link Reducer}s.
 */
public class GoraReducer<K1, V1, K2, V2 extends Persistent>
extends Reducer<K1, V1, K2, V2> {

  public static <K1, V1, K2, V2 extends Persistent>
  void initReducerJob(Job job, DataStore<K2,V2> dataStore,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass) {
    initReducerJob(job, dataStore, reducerClass, true);
  }

  public static <K1, V1, K2, V2 extends Persistent>
  void initReducerJob(Job job, DataStore<K2,V2> dataStore,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass,
          boolean reuseObjects) {
    
    Configuration conf = job.getConfiguration();
    
    GoraMapReduceUtils.setIOSerializations(conf, reuseObjects);
    
    job.setOutputFormatClass(GoraOutputFormat.class);
    job.setReducerClass(reducerClass);
    conf.setClass(GoraOutputFormat.DATA_STORE_CLASS
        , dataStore.getClass(), DataStore.class);
    conf.setClass(GoraOutputFormat.REDUCE_KEY_CLASS,
        dataStore.getKeyClass(), Object.class);
    conf.setClass(GoraOutputFormat.REDUCE_VALUE_CLASS, 
        dataStore.getPersistentClass(), Persistent.class);
  }
}
