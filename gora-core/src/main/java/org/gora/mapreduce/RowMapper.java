package org.gora.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.gora.TableRow;

public class RowMapper<K1, V1 extends TableRow, K2, V2>
extends Mapper<K1, V1, K2, V2> {

  public static <K1, V1 extends TableRow, K2, V2>
  void initRowMapperJob(Job job,
      Class<K1> keyClass, Class<V1> valueClass,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends RowMapper<K1, V1, K2, V2>> mapperClass, String[] fields,
      Class<? extends Partitioner<K2, V2>> partitionerClass) {
    job.setInputFormatClass(RowInputFormat.class);
    job.setMapperClass(mapperClass);
    job.setMapOutputKeyClass(outKeyClass);
    job.setMapOutputValueClass(outValueClass);
    job.getConfiguration().setClass(RowInputFormat.MAP_KEY_CLASS,
        keyClass, Object.class);
    job.getConfiguration().setClass(RowInputFormat.MAP_VALUE_CLASS,
        valueClass, TableRow.class);
    job.getConfiguration().setStrings(RowInputFormat.MAPRED_FIELDS, fields);
    job.getConfiguration().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        TableRowSerialization.class.getCanonicalName());
    if (partitionerClass != null) {
      job.setPartitionerClass(partitionerClass);
    }
  }
  
  public static <K1, V1 extends TableRow, K2, V2>
  void initRowMapperJob(Job job,Class<K1> keyClass, Class<V1> valueClass,
      Class<K2> outKeyClass, Class<V2> outValueClass,
      Class<? extends RowMapper<K1, V1, K2, V2>> mapperClass, String[] fields) {
    initRowMapperJob(job, keyClass, valueClass, outKeyClass, outValueClass,
        mapperClass, fields, null);
  }
}
