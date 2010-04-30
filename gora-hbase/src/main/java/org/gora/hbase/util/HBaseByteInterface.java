
package org.gora.hbase.util;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Contains utility methods for byte[] <-> field 
 * conversions.
 */
public class HBaseByteInterface {
  
  public static final byte[] EMPTY_BYTES = new byte[0]; 
  
  @SuppressWarnings("unchecked")
  public static Object fromBytes(Schema schema, byte[] val) {
    Type type = schema.getType();
    switch (type) {
    case ENUM:
      String symbol = schema.getEnumSymbols().get(val[0]);
      return Enum.valueOf(ReflectData.get().getClass(schema), symbol);
    case STRING:  return new Utf8(Bytes.toString(val));
    case BYTES:   return ByteBuffer.wrap(val);
    case INT:     return Bytes.toInt(val);
    case LONG:    return Bytes.toLong(val);
    case FLOAT:   return Bytes.toFloat(val);
    case DOUBLE:  return Bytes.toDouble(val);
    case BOOLEAN: return val[0] != 0;
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }

  @SuppressWarnings("unchecked")
  public static <K> K fromBytes(Class<K> clazz, byte[] val) {
    if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return (K) Byte.valueOf(val[0]);
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return (K) Boolean.valueOf(val[0] == 0 ? false : true);
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return (K) Short.valueOf(Bytes.toShort(val));
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return (K) Integer.valueOf(Bytes.toInt(val));
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return (K) Long.valueOf(Bytes.toLong(val));
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return (K) Float.valueOf(Bytes.toFloat(val));
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return (K) Double.valueOf(Bytes.toDouble(val));
    } else if (clazz.equals(String.class)) {
      return (K) Bytes.toString(val);
    } else if (clazz.equals(Utf8.class)) {
      return (K) new Utf8(Bytes.toString(val));
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  public static byte[] toBytes(Object o) {
    Class<?> clazz = o.getClass();
    if (clazz.equals(Enum.class)) {
      return new byte[] { (byte)((Enum<?>) o).ordinal() }; // yeah, yeah it's a hack
    } else if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return new byte[] { (Byte) o };
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return new byte[] { ((Boolean) o ? (byte) 1 :(byte) 0)};
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return Bytes.toBytes((Short) o);
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return Bytes.toBytes((Integer) o);
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return Bytes.toBytes((Long) o);
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return Bytes.toBytes((Float) o);
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return Bytes.toBytes((Double) o);
    } else if (clazz.equals(String.class)) {
      return Bytes.toBytes((String) o);
    } else if (clazz.equals(Utf8.class)) {
      return ((Utf8) o).getBytes();
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  public static byte[] toBytes(Object o, Schema schema) {
    Type type = schema.getType();
    switch (type) {
    case STRING:  return Bytes.toBytes(((Utf8)o).toString()); // TODO: maybe ((Utf8)o).getBytes(); ?
    case BYTES:   return ((ByteBuffer)o).array();
    case INT:     return Bytes.toBytes((Integer)o);
    case LONG:    return Bytes.toBytes((Long)o);
    case FLOAT:   return Bytes.toBytes((Float)o);
    case DOUBLE:  return Bytes.toBytes((Double)o);
    case BOOLEAN: return (Boolean)o ? new byte[] {1} : new byte[] {0};
    case ENUM:    return new byte[] { (byte)((Enum<?>) o).ordinal() };
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }
}
