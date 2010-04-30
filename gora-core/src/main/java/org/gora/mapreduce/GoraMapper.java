package org.gora.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.store.DataStore;
import org.gora.util.StringUtils;

/**
 * Optional base class for gora based {@link Mapper}s.
 */
public class GoraMapper<K1, V1 extends Persistent, K2, V2>
extends Mapper<K1, V1, K2, V2> {

  @SuppressWarnings("unchecked")
  public static <K1, V1 extends Persistent, K2, V2>
  void initMapperJob(Job job, Query<K1,V1> query,
      DataStore<K1,V1> dataStore,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends GoraMapper> mapperClass,
      Class<? extends Partitioner> partitionerClass, boolean reuseObjects) 
  throws IOException {
    
    Configuration conf = job.getConfiguration();
    
    setIOSerializations(conf, reuseObjects);
    
    job.setInputFormatClass(GoraInputFormat.class);
    GoraInputFormat.setQuery(job, query);
    job.setMapperClass(mapperClass);
    job.setMapOutputKeyClass(outKeyClass);
    job.setMapOutputValueClass(outValueClass);
    conf.setClass(GoraInputFormat.MAP_KEY_CLASS,
        dataStore.getKeyClass(), Object.class);
    conf.setClass(GoraInputFormat.MAP_VALUE_CLASS,
        dataStore.getPersistentClass(), Persistent.class);
    
    if (partitionerClass != null) {
      job.setPartitionerClass(partitionerClass);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <K1, V1 extends Persistent, K2, V2>
  void initMapperJob(Job job, Query<K1,V1> query, DataStore<K1,V1> dataStore,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends GoraMapper> mapperClass, boolean reuseObjects) 
  throws IOException {
    
    initMapperJob(job, query, dataStore, outKeyClass, outValueClass,
        mapperClass, null, reuseObjects);
  }
  
  static void setIOSerializations(Configuration conf, boolean reuseObjects) {
    String serializationClass =
      PersistentSerialization.class.getCanonicalName();
    if (!reuseObjects) {
      serializationClass =
        PersistentNonReusingSerialization.class.getCanonicalName();
    }
    String[] serializations = StringUtils.joinStringArrays(
        conf.getStrings("io.serializations"), 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        serializationClass); 
    conf.setStrings("io.serializations", serializations);
  }
}
