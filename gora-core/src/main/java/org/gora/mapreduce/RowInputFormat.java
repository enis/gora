package org.gora.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gora.RowScanner;
import org.gora.TableRow;
import org.gora.store.TableSerializer;
import org.gora.store.TableSerializerFactory;

public class RowInputFormat<K, R extends TableRow>
extends InputFormat<K, R> implements Configurable {

  public static final String MAPRED_FIELDS   = "storage.mapred.fields";

  public static final String MAP_KEY_CLASS   = "storage.map.key.class";

  public static final String MAP_VALUE_CLASS = "storage.map.value.class";

  private TableSerializer<K, R> serializer;

  private Configuration conf;

  @Override
  public RecordReader<K, R> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    String[] fields = context.getConfiguration().getStrings(MAPRED_FIELDS);
    final RowScanner<K, R> scanner = serializer.makeScanner(split, fields);
    return new RecordReader<K, R>() {
      private K key;
      private R row;

      @Override
      public void close() throws IOException {
        scanner.close();
      }

      @Override
      public K getCurrentKey() throws IOException, InterruptedException {
        return key;
      }

      @Override
      public R getCurrentValue() throws IOException, InterruptedException {
        return row;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException { }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        Entry<K, R> entry = scanner.next();
        if (entry == null) {
          return false;
        }
        key = entry.getKey();
        row = entry.getValue();
        return true;
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return serializer.getSplits(null, null, context);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    Class<K> keyClass = (Class<K>) conf.getClass(MAP_KEY_CLASS, null);
    Class<R> rowClass = (Class<R>) conf.getClass(MAP_VALUE_CLASS, null);
    this.serializer = TableSerializerFactory.create(conf, keyClass, rowClass);
  }

}
