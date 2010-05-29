
package org.gora.store.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.gora.mapreduce.GoraMapReduceUtils;
import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.query.impl.FileSplitPartitionQuery;
import org.gora.store.DataStoreFactory;
import org.gora.store.FileBackedDataStore;
import org.gora.util.OperationNotSupportedException;

/**
 * Base implementations for {@link FileBackedDataStore} methods.
 */
public abstract class FileBackedDataStoreBase<K, T extends Persistent> 
  extends DataStoreBase<K, T> implements FileBackedDataStore<K, T> {

  protected long inputSize; //input size in bytes
  
  protected String inputPath;
  protected String outputPath;
  
  protected InputStream inputStream;
  protected OutputStream outputStream;
  
  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException {
    super.initialize(keyClass, persistentClass, properties);
    if(properties != null) {
      if(this.inputPath == null) {
        this.inputPath = DataStoreFactory.getInputPath(properties, this);
      }
      if(this.outputPath == null) {
        this.outputPath = DataStoreFactory.getOutputPath(properties, this);
      }
    }
  }
  
  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }
  
  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }
  
  public String getInputPath() {
    return inputPath;
  }
  
  public String getOutputPath() {
    return outputPath;
  }
  
  public void setInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }
  
  public void setOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }
  
  public InputStream getInputStream() {
    return inputStream;
  }
  
  @Override
  public OutputStream getOutputStream() {
    return outputStream;
  }
  
  /** Opens an InputStream for the input Hadoop path */ 
  protected InputStream createInputStream() throws IOException {
    //TODO: if input path is a directory, use smt like MultiInputStream to
    //read all the files recursively
    Path path = new Path(inputPath);
    FileSystem fs = path.getFileSystem(getConf());
    inputSize = fs.getFileStatus(path).getLen();
    return fs.open(path);
  }
  
  /** Opens an OutputStream for the output Hadoop path */ 
  protected OutputStream createOutputStream() throws IOException {
    Path path = new Path(outputPath);
    FileSystem fs = path.getFileSystem(getConf());
    return fs.create(path);
  }

  protected InputStream getOrCreateInputStream() throws IOException {
    if(inputStream == null) {
      inputStream = createInputStream();
    }
    return inputStream;
  }

  protected OutputStream getOrCreateOutputStream() throws IOException {
    if(outputStream == null) {
      outputStream = createOutputStream();
    }
    return outputStream;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    List<InputSplit> splits = GoraMapReduceUtils.getSplits(getConf(), inputPath);
    List<PartitionQuery<K, T>> queries = new ArrayList<PartitionQuery<K,T>>(splits.size());
    
    for(InputSplit split : splits) {
      queries.add(new FileSplitPartitionQuery<K, T>(query, (FileSplit) split));
    }
    
    return queries; 
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public Result<K, T> execute(Query<K, T> query) throws IOException {
    if(query instanceof FileSplitPartitionQuery) {
        return executePartial((FileSplitPartitionQuery<K, T>) query);
    } else {
      return executeQuery(query);
    }
  }
  
  /**
   * Executes a normal Query reading the whole data. #execute() calls this function
   * for non-PartitionQuery's.
   */
  protected abstract Result<K,T> executeQuery(Query<K,T> query) 
    throws IOException;
  
  /**
   * Executes a PartitialQuery, reading the data between start and end.
   */
  protected abstract Result<K,T> executePartial(FileSplitPartitionQuery<K,T> query) 
    throws IOException;
  
  @Override
  public void flush() throws IOException {
    if(outputStream != null)
      outputStream.flush();
  }
  
  @Override
  public void createSchema() throws IOException {
  }
  
  @Override
  public void deleteSchema() throws IOException {
    throw new OperationNotSupportedException("delete schema is not supported for " +
    		"file backed data stores");
  }
  
  @Override
  public boolean schemaExists() throws IOException {
    return true;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    org.gora.util.IOUtils.writeNullFieldsInfo(out, inputPath, outputPath);
    if(inputPath != null)
      Text.writeString(out, inputPath);
    if(outputPath != null)
      Text.writeString(out, outputPath);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    boolean[] nullFields = org.gora.util.IOUtils.readNullFieldsInfo(in);
    if(!nullFields[0]) 
      inputPath = Text.readString(in);
    if(!nullFields[1]) 
      outputPath = Text.readString(in);
  }
  
  @Override
  public void close() throws IOException {
    IOUtils.closeStream(inputStream);
    IOUtils.closeStream(outputStream);
    inputStream = null;
    outputStream = null;
  }
}
