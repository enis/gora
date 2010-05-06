
package org.gora.avro.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.gora.avro.query.AvroQuery;
import org.gora.avro.query.AvroResult;
import org.gora.persistency.Persistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.impl.DataStoreBase;
import org.gora.util.OperationNotSupportedException;

/**
 * An adapter DataStore for binary-compatible Avro serializations. 
 * AvroDataStore supports Binary and JSON serializations.
 * @param <T>
 */
public class AvroStore<K, T extends Persistent> 
  extends DataStoreBase<K, T> implements Configurable {

  private Configuration conf;
  
  /**
   * The property key for the input path. The file under this path is opened 
   * for reading  using Hadoop {@link FileSystem} API.
   */
  public static final String INPUT_PATH_KEY = "gora.avrostore.input.path";
  
  /**
   * The property key for the output path. The file under this path is opened 
   * for writing  using Hadoop {@link FileSystem} API.
   */
  public static final String OUTPUT_PATH_KEY = "gora.avrostore.output.path";
  
  /** The property key specifying avro encoder/decoder type to use. Can take values
   * "BINARY" or "JSON". */
  public static final String CODEC_TYPE_KEY = "gora.avrostore.codec.type";
  
  /**
   * The type of the avro Encoder/Decoder.
   */
  public static enum CodecType {
    /** Avro binary encoder */
    BINARY, 
    /** Avro JSON encoder */
    JSON,
  }
  
  protected String inputPath;
  protected String outputPath;
  
  protected long inputSize; //input size in bytes
  
  private InputStream inputStream;
  private OutputStream outputStream;
  
  private DatumReader<T> datumReader;
  private DatumWriter<T> datumWriter;
  private Encoder encoder;
  private Decoder decoder;
  
  private CodecType codecType = CodecType.JSON;
  
  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException {
    super.initialize(keyClass, persistentClass, properties);
   
    if(properties != null) {
      if(this.codecType == null) {
        String codecType = properties.getProperty(CODEC_TYPE_KEY, "BINARY");
        this.codecType = CodecType.valueOf(codecType);
      }
      
      if(this.inputPath == null) {
        this.inputPath = properties.getProperty(INPUT_PATH_KEY);
      }
      if(this.outputPath == null) {
        this.outputPath = properties.getProperty(OUTPUT_PATH_KEY);
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
  
  public void setCodecType(CodecType codecType) {
    this.codecType = codecType;
  }
  
  public void setEncoder(Encoder encoder) {
    this.encoder = encoder;
  }
  
  public void setDecoder(Decoder decoder) {
    this.decoder = decoder;
  }
  
  public void setDatumReader(DatumReader<T> datumReader) {
    this.datumReader = datumReader;
  }
  
  public void setDatumWriter(DatumWriter<T> datumWriter) {
    this.datumWriter = datumWriter;
  }
  
  @Override
  public void createSchema() throws IOException {
  }
  
  @Override
  public void close() throws IOException {
    if(encoder != null) {
      encoder.flush();
    }
    IOUtils.closeStream(inputStream);
    IOUtils.closeStream(outputStream);
  }

  @Override
  public void delete(K key) throws IOException {
    throw new OperationNotSupportedException();
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) throws IOException {
    return new AvroResult<K,T>(this, (AvroQuery<K,T>) query, getDatumReader(), getDecoder());
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
    encoder.flush();
  }

  @Override
  public T get(K key, String[] fields) throws IOException {
    return null;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    return null;
  }

  @Override
  public AvroQuery<K,T> newQuery() {
    return new AvroQuery<K,T>(this);
  }

  @Override
  public void put(K key, T obj) throws IOException {
    getDatumWriter().write(obj, getEncoder());
  }
  
  protected InputStream getInputStream() throws IOException {
    if(inputStream == null) {
      inputStream = createInputStream();
    }
    return inputStream;
  }
  
  protected OutputStream getOutputStream() throws IOException {
    if(outputStream == null) {
      outputStream = createOutputStream();
    }
    return outputStream;
  }
  
  public Encoder getEncoder() throws IOException {
    if(encoder == null) {
      encoder = createEncoder();
    }
    return encoder;
  }
  
  public Decoder getDecoder() throws IOException {
    if(decoder == null) {
      decoder = createDecoder();
    }
    return decoder;
  }
  
  public DatumReader<T> getDatumReader() {
    if(datumReader == null) {
      datumReader = createDatumReader();
    }
    return datumReader;
  }
  
  public DatumWriter<T> getDatumWriter() {
    if(datumWriter == null) {
      datumWriter = createDatumWriter();
    }
    return datumWriter;
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
  
  protected Encoder createEncoder() throws IOException {
    switch(codecType) {
      case BINARY:
        return new BinaryEncoder(getOutputStream());
      case JSON:
        return new JsonEncoder(schema, getOutputStream());
    }
    return null;
  }
  
  protected Decoder createDecoder() throws IOException {
    switch(codecType) {
      case BINARY:
        return new BinaryDecoder(getInputStream());
      case JSON:
        return new JsonDecoder(schema, getInputStream());
    }
    return null;
  }
  
  protected DatumWriter createDatumWriter() {
    return new SpecificDatumWriter(schema);
  }
  
  protected DatumReader createDatumReader() {
    return new SpecificDatumReader(schema);
  }
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public Configuration getConf() {
    if(conf == null) {
      conf = new Configuration();
    }
    return conf;
  }
}
