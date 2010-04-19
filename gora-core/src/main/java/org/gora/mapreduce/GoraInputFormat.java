package org.gora.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;

public class GoraInputFormat<K, T extends Persistent>
extends InputFormat<K, T> implements Configurable {

  public static final String MAPRED_FIELDS   = "gora.inputformat.mapred.fields";

  public static final String MAP_KEY_CLASS   = "gora.inputformat.map.key.class";

  public static final String MAP_VALUE_CLASS = "gora.inputformat.map.value.class";

  private DataStore<K, T> dataStore;

  private Configuration conf;

  private Query<K, T> query;
  
  @Override
  public RecordReader<K, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new GoraRecordReader<K, T>(query);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    
    List<PartitionQuery<K, T>> queries = dataStore.getPartitions(query);
    List<InputSplit> splits = new ArrayList<InputSplit>(queries.size());
    
    for(PartitionQuery<K,T> query : queries) {
      splits.add(new GoraInputSplit(query));
    }
    
    return splits;
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
    Class<T> rowClass = (Class<T>) conf.getClass(MAP_VALUE_CLASS, null);
    this.dataStore = new DataStoreFactory().getDataStore(keyClass, rowClass);
    //TODO: query should be read from the serialized form
    String[] fields = conf.getStrings(MAPRED_FIELDS);
    query = dataStore.newQuery();
    query.setFields(fields);
  }
}
