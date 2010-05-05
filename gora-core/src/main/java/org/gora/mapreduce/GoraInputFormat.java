package org.gora.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.store.DataStore;
import org.mortbay.log.Log;

/**
 * {@link InputFormat} to fetch the input from gora data stores. The 
 * query to fetch the items from the datastore should be prepared and 
 * set via {@link #setQuery(Job, Query)}, before submitting the job. 
 * 
 * <p> The {@link InputSplit}s are prepared from the {@link PartitionQuery}s  
 * obtained by calling {@link DataStore#getPartitions(Query)}.
 */
public class GoraInputFormat<K, T extends Persistent> 
  extends InputFormat<K, T> implements Configurable {

  public static final String QUERY_KEY   = "gora.inputformat.query";
  
  public static final String QUERY_CLASS_KEY = "gora.inputformat.query.class";

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
    
    String queryClass = conf.get(QUERY_CLASS_KEY);
    try {
      query = (Query<K, T>) DefaultStringifier.load(conf, 
          QUERY_KEY, Class.forName(queryClass));
    } catch (Exception ex) {
      Log.warn(StringUtils.stringifyException(ex));
      throw new RuntimeException(ex);
    }
    this.dataStore = query.getDataStore();
  }
  
  public static<K, T extends Persistent> void setQuery(Job job
      , Query<K, T> query) throws IOException {
    job.getConfiguration().set(QUERY_CLASS_KEY, query.getClass().getCanonicalName());
    DefaultStringifier.store(job.getConfiguration(), query, QUERY_KEY);
  }
  
  /**
   * Sets the input parameters for the job 
   * @param job the job to set the properties for
   * @param query the query to get the inputs from
   * @param dataStore the datastore as the input
   * @param reuseObjects whether to reuse objects in serialization
   * @throws IOException
   */
  public static <K1, V1 extends Persistent> void setInput(Job job
      , Query<K1,V1> query, DataStore<K1,V1> dataStore, boolean reuseObjects) 
  throws IOException {
    
    Configuration conf = job.getConfiguration();
    
    GoraMapReduceUtils.setIOSerializations(conf, reuseObjects);
    
    job.setInputFormatClass(GoraInputFormat.class);
    GoraInputFormat.setQuery(job, query);
  }
  
}
