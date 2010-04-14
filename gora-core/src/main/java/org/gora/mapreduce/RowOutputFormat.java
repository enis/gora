package org.gora.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gora.TableRow;
import org.gora.store.TableSerializer;
import org.gora.store.TableSerializerFactory;

public class RowOutputFormat<K, R extends TableRow>
extends OutputFormat<K, R>{

  public static final String REDUCE_KEY_CLASS   = "storage.reduce.key.class";

  public static final String REDUCE_VALUE_CLASS = "storage.reduce.value.class";

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
  public RecordWriter<K, R> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Class<K> keyClass = (Class<K>) conf.getClass(REDUCE_KEY_CLASS, null);
    Class<R> rowClass = (Class<R>) conf.getClass(REDUCE_VALUE_CLASS, null);
    final TableSerializer<K, R> serializer =
      TableSerializerFactory.create(context.getConfiguration(), keyClass, rowClass);
    return new RecordWriter<K, R>() {
      @Override
      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        serializer.sync();
      }

      @Override
      public void write(K key, R row)
      throws IOException, InterruptedException {
        serializer.updateRow(key, row);
      }
    };
  }

}
