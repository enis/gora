
package org.gora.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.gora.query.PartitionQuery;

/**
 * InputSplit using {@link PartitionQuery}s. 
 */
public class GoraInputSplit extends InputSplit implements Writable {

  protected PartitionQuery<?,?> query;
  
  public GoraInputSplit() {
  }
  
  public GoraInputSplit(PartitionQuery<?,?> query) {
    this.query = query;
  }
  
  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return query.getLocations();
  }

  public PartitionQuery<?, ?> getQuery() {
    return query;
  }
  
  @Override
  public void readFields(DataInput arg0) throws IOException {
    //TODO
  }
  
  @Override
  public void write(DataOutput arg0) throws IOException {
    //TODO
  }
  
}
