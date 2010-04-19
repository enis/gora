package org.gora.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.gora.persistency.Persistent;

public class GoraReducer<K1, V1, K2, V2 extends Persistent>
extends Reducer<K1, V1, K2, V2> {

  public static <K1, V1, K2, V2 extends Persistent>
  void initRowReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass) {
    initRowReducerJob(job, keyClass, valueClass, reducerClass, true);
  }

  public static <K1, V1, K2, V2 extends Persistent>
  void initRowReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends GoraReducer<K1, V1, K2, V2>> reducerClass,
          boolean reuseOld) {
    String tableSerializationClass =
      PersistentSerialization.class.getCanonicalName();
    if (!reuseOld) {
      tableSerializationClass =
        PersistentNonReusingSerialization.class.getCanonicalName();
    }
    job.getConfiguration().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        tableSerializationClass);
    job.setOutputFormatClass(GoraOutputFormat.class);
    job.setReducerClass(reducerClass);
    job.getConfiguration().setClass(GoraOutputFormat.REDUCE_KEY_CLASS,
        keyClass, Object.class);
    job.getConfiguration().setClass(GoraOutputFormat.REDUCE_VALUE_CLASS,
        valueClass, Persistent.class);

  }
}
