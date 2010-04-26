package org.gora.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.util.StringUtils;

/**
 * Optional base class for gora based {@link Mapper}s.
 */
public class GoraMapper<K1, V1 extends Persistent, K2, V2>
extends Mapper<K1, V1, K2, V2> {

  public static <K1, V1 extends Persistent, K2, V2>
  void initMapperJob(Job job, Query<K1,V1> query,
      Class<K1> keyClass, Class<V1> valueClass,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends GoraMapper<K1, V1, K2, V2>> mapperClass,
      Class<? extends Partitioner<K2, V2>> partitionerClass, boolean reuseObjects) 
  throws IOException {
    
    Configuration conf = job.getConfiguration();
    
    setIOSerializations(conf, reuseObjects);
    
    job.setInputFormatClass(GoraInputFormat.class);
    GoraInputFormat.setQuery(job, query);
    job.setMapperClass(mapperClass);
    job.setMapOutputKeyClass(outKeyClass);
    job.setMapOutputValueClass(outValueClass);
    conf.setClass(GoraInputFormat.MAP_KEY_CLASS,
        keyClass, Object.class);
    conf.setClass(GoraInputFormat.MAP_VALUE_CLASS,
        valueClass, Persistent.class);
    
    if (partitionerClass != null) {
      job.setPartitionerClass(partitionerClass);
    }
  }
  
  public static <K1, V1 extends Persistent, K2, V2>
  void initMapperJob(Job job, Query<K1,V1> query, Class<K1> keyClass, Class<V1> valueClass,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends GoraMapper<K1, V1, K2, V2>> mapperClass, boolean reuseObjects) 
  throws IOException {
    
    initMapperJob(job, query, keyClass, valueClass, outKeyClass, outValueClass,
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
