package org.gora.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.gora.persistency.Persistent;

/**
 * Optional base class for gora based {@link Reducer}s.
 */
public class GoraReducer<K1, V1, K2, V2 extends Persistent>
extends Reducer<K1, V1, K2, V2> {

  public static <K1, V1, K2, V2 extends Persistent>
  void initReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass) {
    initReducerJob(job, keyClass, valueClass, reducerClass, true);
  }

  public static <K1, V1, K2, V2 extends Persistent>
  void initReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass,
          boolean reuseObjects) {
    
    Configuration conf = job.getConfiguration();
    
    GoraMapper.setIOSerializations(conf, reuseObjects);
    
    job.setOutputFormatClass(GoraOutputFormat.class);
    job.setReducerClass(reducerClass);
    conf.setClass(GoraOutputFormat.REDUCE_KEY_CLASS,
        keyClass, Object.class);
    conf.setClass(GoraOutputFormat.REDUCE_VALUE_CLASS, 
        valueClass, Persistent.class);
  }
}
