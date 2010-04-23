
package org.gora.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * An utility class for I/O related functionality. 
 */
public class IOUtils {

  private static SerializationFactory serializationFactory = null;
  private static Configuration conf;
  
  private static Configuration getOrCreateConf(Configuration conf) {
    if(conf == null) {
      if(IOUtils.conf == null) {
        IOUtils.conf = new Configuration();
      }
    }
    return conf != null ? conf : IOUtils.conf;
  }
  
  public static Object readObject(DataInput in) 
    throws ClassNotFoundException, IOException {
    
    if(in instanceof ObjectInput) {
      return ((ObjectInput)in).readObject();
    } else {
      if(in instanceof InputStream) {
        ObjectInput objIn = new ObjectInputStream((InputStream)in);
        Object obj = objIn.readObject();
        return obj;
      }
    }
    throw new IOException("cannot write to DataOutput of instance:" 
        + in.getClass());
  }
  
  public static void writeObject(DataOutput out, Object obj) 
    throws IOException {
    if(out instanceof ObjectOutput) {
      ((ObjectOutput)out).writeObject(obj);
    } else {
      if(out instanceof OutputStream) {
        ObjectOutput objOut = new ObjectOutputStream((OutputStream)out);
        objOut.writeObject(obj);
      }
    }
    throw new IOException("cannot write to DataOutput of instance:" 
        + out.getClass());
  }
  
  /** Serializes the object to the given dataoutput using 
   * available Hadoop serializations
   * @throws IOException */
  public static<T> void serialize(Configuration conf, DataOutput out
      , T obj, Class<T> objClass) throws IOException {
    
    if(serializationFactory == null) {
      serializationFactory = new SerializationFactory(getOrCreateConf(conf));
    }
    Serializer<T> serializer = serializationFactory.getSerializer(objClass);
    
    ByteBufferOutputStream os = new ByteBufferOutputStream();
    try {
      serializer.open(os);
      serializer.serialize(obj);
      
      int length = 0;
      List<ByteBuffer> buffers = os.getBufferList();
      for(ByteBuffer buffer : buffers) {
        length += buffer.limit() - buffer.arrayOffset();
      }
      
      WritableUtils.writeVInt(out, length);
      for(ByteBuffer buffer : buffers) {
        byte[] arr = buffer.array();
        out.write(arr, buffer.arrayOffset(), buffer.limit());
      }
      
    }finally {
      if(serializer != null)
        serializer.close();
      if(os != null)
        os.close();
    }
  }
  
  /** Serializes the object to the given dataoutput using 
   * available Hadoop serializations
   * @throws IOException */
  @SuppressWarnings("unchecked")
  public static<T> void serialize(Configuration conf, DataOutput out
      , T obj) throws IOException {
    Text.writeString(out, obj.getClass().getCanonicalName());
    serialize(conf, out, obj, (Class<T>)obj.getClass());
  }
  
  /** Deserializes the object in the given datainput using 
   * available Hadoop serializations.
   * @throws IOException 
   * @throws ClassNotFoundException */
  @SuppressWarnings("unchecked")
  public static<T> T deserialize(Configuration conf, DataInput in
      , T obj , String objClass) throws IOException, ClassNotFoundException {
    
    Class<T> c = (Class<T>) Class.forName(objClass);
    
    return deserialize(conf, in, obj, c);
  }
  
  /** Deserializes the object in the given datainput using 
   * available Hadoop serializations.
   * @throws IOException */
  public static<T> T deserialize(Configuration conf, DataInput in
      , T obj , Class<T> objClass) throws IOException {
    if(serializationFactory == null) {
      serializationFactory = new SerializationFactory(getOrCreateConf(conf));
    }
    Deserializer<T> deserializer = serializationFactory.getDeserializer(
        objClass);
    
    int length = WritableUtils.readVInt(in);
    byte[] arr = new byte[length];
    in.readFully(arr);
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    list.add(ByteBuffer.wrap(arr));
    ByteBufferInputStream is = new ByteBufferInputStream(list);
    
    try {
      deserializer.open(is);
      T newObj = deserializer.deserialize(obj);
      return newObj;
      
    }finally {
      if(deserializer != null)
        deserializer.close();
      if(is != null)
        is.close();
    }
  }
  
  /** Deserializes the object in the given datainput using 
   * available Hadoop serializations.
   * @throws IOException 
   * @throws ClassNotFoundException */
  @SuppressWarnings("unchecked")
  public static<T> T deserialize(Configuration conf, DataInput in
      , T obj) throws IOException, ClassNotFoundException {
    String clazz = Text.readString(in);
    Class<T> c = (Class<T>)Class.forName(clazz);
    return deserialize(conf, in, obj, c);
  }
  
  /**
   * Writes a byte[] to the output, representing whether each given field is null
   * or not. A Vint and ceil( fields.length / 8 ) bytes are written to the output.
   * @param out the output to write to
   * @param fields the fields to check for null
   * @see #readNullFieldsInfo(DataInput)
   */
  public static void writeNullFieldsInfo(DataOutput out, Object ... fields) 
    throws IOException {

    boolean[] isNull = new boolean[fields.length];

    for(int i=0; i<fields.length; i++) {
      isNull[i] = (fields[i] == null);
    }

    writeBoolArray(out, isNull);
  }

  /**
   * Reads the data written by {@link #writeNullFieldsInfo(DataOutput, Object...)}
   * and returns a boolean array representing whether each field is null or not.
   * @param in the input to read from
   * @return a boolean[] representing whether each field is null or not.
   */
  public static boolean[] readNullFieldsInfo(DataInput in) throws IOException {
    return readBoolArray(in);
  }
  
  /**
   * Writes a boolean[] to the output.
   */
  public static void writeBoolArray(DataOutput out, boolean[] boolArray) 
    throws IOException {
    
    WritableUtils.writeVInt(out, boolArray.length);
    
    byte b = 0;
    int i = 0;
    for(i=0; i<boolArray.length; i++) {
      if(i % 8 == 0 && i != 0) {
        out.writeByte(b);
        b = 0;
      }
      b >>= 1;
      if(boolArray[i])
        b |= 0x80;
      else
        b &= 0x7F;
    }
    if(i % 8 != 0) {
      for(int j=0; j < 8 - (i % 8); j++) { //shift for the remaining byte
        b >>=1;
        b &= 0x7F;
      }
    }
    
    out.writeByte(b);
  }

  /**
   * Reads a boolean[] from input
   * @throws IOException 
   */
  public static boolean[] readBoolArray(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    boolean[] arr = new boolean[length];
    
    byte b = 0;
    for(int i=0; i < length; i++) {
      if(i % 8 == 0) {
        b = in.readByte();
      }
      arr[i] = (b & 0x01) > 0;
      b >>= 1; 
    }
    return arr;
  }
  
  /**
   * Writes the String array to the given DataOutput.
   * @param out the data output to write to
   * @param arr the array to write
   * @see #readStringArray(DataInput)
   */
  public static void writeStringArray(DataOutput out, String[] arr)
    throws IOException {
    WritableUtils.writeVInt(out, arr.length);
    for(String str : arr) {
      Text.writeString(out, str);
    }
  }

  /**
   * Reads and returns a String array that is written by
   * {@link #writeStringArray(DataOutput, String[])}.
   * @param in the data input to read from
   * @return read String[]
   */
  public static String[] readStringArray(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    String[] arr = new String[len];
    for(int i=0; i<len; i++) {
      arr[i] = Text.readString(in);
    }
    return arr;
  }
  
}