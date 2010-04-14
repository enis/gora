package org.gora.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.gora.TableRow;

public class RowReducer<K1, V1, K2, V2 extends TableRow>
extends Reducer<K1, V1, K2, V2> {

  public static <K1, V1, K2, V2 extends TableRow>
  void initRowReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends RowReducer<K1, V1, K2, V2>> reducerClass) {
    initRowReducerJob(job, keyClass, valueClass, reducerClass, true);
  }

  public static <K1, V1, K2, V2 extends TableRow>
  void initRowReducerJob(Job job, Class<K2> keyClass, Class<V2> valueClass,
      Class<? extends RowReducer<K1, V1, K2, V2>> reducerClass,
          boolean reuseOld) {
    String tableSerializationClass =
      TableRowSerialization.class.getCanonicalName();
    if (!reuseOld) {
      tableSerializationClass =
        TableRowNonReusingSerialization.class.getCanonicalName();
    }
    job.getConfiguration().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        tableSerializationClass);
    job.setOutputFormatClass(RowOutputFormat.class);
    job.setReducerClass(reducerClass);
    job.getConfiguration().setClass(RowOutputFormat.REDUCE_KEY_CLASS,
        keyClass, Object.class);
    job.getConfiguration().setClass(RowOutputFormat.REDUCE_VALUE_CLASS,
        valueClass, TableRow.class);

  }
}
