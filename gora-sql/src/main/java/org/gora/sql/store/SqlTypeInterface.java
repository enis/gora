
package org.gora.sql.store;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Currency;
import java.util.Locale;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public class SqlTypeInterface {
  
  public static int getSqlType(Class<?> clazz) {
    
    //jdo default types 
    if (Boolean.class.isAssignableFrom(clazz)) {
      return Types.BIT;
    } else if (Character.class.isAssignableFrom(clazz)) {
      return Types.CHAR;
    } else if (Byte.class.isAssignableFrom(clazz)) {
      return Types.TINYINT;
    } else if (Short.class.isAssignableFrom(clazz)) {
      return Types.SMALLINT;
    } else if (Integer.class.isAssignableFrom(clazz)) {
      return Types.INTEGER;
    } else if (Long.class.isAssignableFrom(clazz)) {
      return Types.BIGINT;
    } else if (Float.class.isAssignableFrom(clazz)) {
      return Types.FLOAT;
    } else if (Double.class.isAssignableFrom(clazz)) {
      return Types.DOUBLE;
    } else if (java.util.Date.class.isAssignableFrom(clazz)) {
      return Types.TIMESTAMP;
    } else if (java.sql.Date.class.isAssignableFrom(clazz)) {
      return Types.DATE;
    } else if (java.sql.Time.class.isAssignableFrom(clazz)) {
      return Types.TIME;
    } else if (java.sql.Timestamp.class.isAssignableFrom(clazz)) {
      return Types.TIMESTAMP;
    } else if (String.class.isAssignableFrom(clazz)) {
      return Types.VARCHAR;
    } else if (Locale.class.isAssignableFrom(clazz)) {
      return Types.VARCHAR;
    } else if (Currency.class.isAssignableFrom(clazz)) {
      return Types.VARCHAR;
    } else if (BigInteger.class.isAssignableFrom(clazz)) {
      return Types.NUMERIC;
    } else if (BigDecimal.class.isAssignableFrom(clazz)) {
      return Types.DECIMAL;
    } else if (Serializable.class.isAssignableFrom(clazz)) {
      return Types.LONGVARBINARY;
    }
    
    //Hadoop types
    else if (DoubleWritable.class.isAssignableFrom(clazz)) {
      return Types.DOUBLE;
    } else if (FloatWritable.class.isAssignableFrom(clazz)) {
      return Types.FLOAT;
    } else if (IntWritable.class.isAssignableFrom(clazz)) {
      return Types.INTEGER;
    } else if (LongWritable.class.isAssignableFrom(clazz)) {
      return Types.BIGINT;
    } else if (Text.class.isAssignableFrom(clazz)) {
      return Types.VARCHAR;
    } else if (VIntWritable.class.isAssignableFrom(clazz)) {
      return Types.INTEGER;
    } else if (VLongWritable.class.isAssignableFrom(clazz)) {
      return Types.BIGINT;
    } else if (Writable.class.isAssignableFrom(clazz)) {
      return Types.LONGVARBINARY;
    }
    
    //avro types
    else if (Utf8.class.isAssignableFrom(clazz)) {
      return Types.VARCHAR;
    }
    
    return Types.OTHER;
  }
  
}
