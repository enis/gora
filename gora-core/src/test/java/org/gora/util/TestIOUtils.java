
package org.gora.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

/**
 * Test case for {@link IOUtils} class.
 */
public class TestIOUtils {

  private static Configuration conf = new Configuration();

  private static final int BOOL_ARRAY_MAX_LENGTH = 30;
  private static final int STRING_ARRAY_MAX_LENGTH = 30;
  
  private static class BoolArrayWrapper implements Writable {
    boolean[] arr;
    @SuppressWarnings("unused")
    public BoolArrayWrapper() {
    }
    public BoolArrayWrapper(boolean[] arr) {
      this.arr = arr;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      this.arr = IOUtils.readBoolArray(in);
    }
    @Override
    public void write(DataOutput out) throws IOException {
      IOUtils.writeBoolArray(out, arr);
    }
    @Override
    public boolean equals(Object obj) {
      return Arrays.equals(arr, ((BoolArrayWrapper)obj).arr);
    }
  }
  
  private static class StringArrayWrapper implements Writable {
    String[] arr;
    @SuppressWarnings("unused")
    public StringArrayWrapper() {
    }
    public StringArrayWrapper(String[] arr) {
      this.arr = arr;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      this.arr = IOUtils.readStringArray(in);
    }
    @Override
    public void write(DataOutput out) throws IOException {
      IOUtils.writeStringArray(out, arr);
    }
    @Override
    public boolean equals(Object obj) {
      return Arrays.equals(arr, ((StringArrayWrapper)obj).arr);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T> void testSerializeDeserialize(T before) throws Exception {
    ByteBufferOutputStream os = new ByteBufferOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ByteBufferInputStream is = null;
    DataInputStream dis = null;
    
    try {
      IOUtils.serialize(conf, dos , before, (Class<T>)before.getClass());
      dos.flush();
      
      is = new ByteBufferInputStream(os.getBufferList());
      dis = new DataInputStream(is);
      
      T after = IOUtils.deserialize(conf, dis, null, (Class<T>)before.getClass());
      
      System.out.println("Before: " + before);
      System.out.println("After : " + after);
      Assert.assertEquals(before, after);
    }finally {
      org.apache.hadoop.io.IOUtils.closeStream(dos);
      org.apache.hadoop.io.IOUtils.closeStream(os);
      org.apache.hadoop.io.IOUtils.closeStream(dis);
      org.apache.hadoop.io.IOUtils.closeStream(is);
    }
  }
  
  @Test
  public void testWritableSerde() throws Exception {
    Text text = new Text("foo goes to a bar to get some buzz");
    testSerializeDeserialize(text);
  }
  
  @Test
  public void testJavaSerializableSerde() throws Exception {
    Integer integer = Integer.valueOf(42);
    testSerializeDeserialize(integer);
  }
  
  @Test
  public void testReadWriteBoolArray() throws Exception {
    
    boolean[][] patterns = {
        {true},
        {false},
        {true, false},
        {false, true},
        {true, false, true},
        {false, true, false},
        {false, true, false, false, true, true, true},
        {false, true, false, false, true, true, true, true},
        {false, true, false, false, true, true, true, true, false},
    };
    
    for(int i=0; i<BOOL_ARRAY_MAX_LENGTH; i++) {
      for(int j=0; j<patterns.length; j++) {
        boolean[] arr = new boolean[i];
        for(int k=0; k<i; k++) {
          arr[k] = patterns[j][k % patterns[j].length];
        }
        
        System.out.println("testing for:" + Arrays.toString(arr));
        testSerializeDeserialize(new BoolArrayWrapper(arr));
      }
    }
  }
  
  @Test
  public void testReadWriteNullFieldsInfo() throws IOException {

    Integer n = null; //null
    Integer nn = new Integer(42); //not null

    testNullFieldsWith(nn);
    testNullFieldsWith(n);
    testNullFieldsWith(n, nn);
    testNullFieldsWith(nn, n);
    testNullFieldsWith(nn, n, nn, n);
    testNullFieldsWith(nn, n, nn, n, n, n, nn, nn, nn, n, n);
  }

  private void testNullFieldsWith( Object ... values ) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    IOUtils.writeNullFieldsInfo(out, values);

    in.reset(out.getData(), out.getLength());

    boolean[] ret = IOUtils.readNullFieldsInfo(in);

    //assert
    Assert.assertEquals(values.length, ret.length);

    for(int i=0; i<values.length; i++) {
      Assert.assertEquals( values[i] == null , ret[i]);
    }
  }
  
  @Test
  public void testReadWriteStringArray() throws Exception {
    for(int i=0; i<STRING_ARRAY_MAX_LENGTH; i++) {
      String[] arr = new String[i];
      for(int j=0; j<i; j++) {
        arr[j] = String.valueOf(j);
      }
      
      System.out.println("testing for:" + Arrays.toString(arr));
      testSerializeDeserialize(new StringArrayWrapper(arr));
    }
  }
}
