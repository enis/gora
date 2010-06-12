
package org.gora.sql.store;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Currency;
import java.util.Locale;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Contains utility methods related to type conversion between 
 * java, avro and SQL types.
 */
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

  public static String getSqlTypeAsString(Schema schema) throws IOException {
    int type = getSqlType(schema);
    return jdbcTypeToString(type);
  }

  public static int getSqlType(Schema schema) throws IOException {
    Type type = schema.getType();

    switch(type) {
      case MAP    : return Types.BLOB;
      case ARRAY  : return Types.BLOB;
      case BOOLEAN: return Types.BIT;
      case BYTES  : return Types.BLOB;
      case DOUBLE : return Types.DOUBLE;
      case ENUM   : return Types.VARCHAR;        
      case FIXED  : return Types.BINARY; 
      case FLOAT  : return Types.FLOAT;
      case INT    : return Types.INTEGER;  
      case LONG   : return Types.BIGINT; 
      case NULL   : break;
      case RECORD : return Types.BLOB;
      case STRING : return Types.VARCHAR;        
      case UNION  : throw new IOException("Union is not supported yet");
    }
    return -1;
  }

  public static String jdbcTypeToString(int type) throws IOException {
    //I don't know whether this exists in jdbc 
    switch(type) {
      case Types.BIT      : return "BIT";
      case Types.BINARY   : return "BINARY";
      case Types.VARBINARY: return "VARBINARY";
      case Types.DOUBLE   : return "DOUBLE";
      case Types.VARCHAR  : return "VARCHAR";
      case Types.FLOAT    : return "FLOAT";
      case Types.INTEGER  : return "INTEGER";
      case Types.BIGINT   : return "BIGINT";
      case Types.BLOB     : return "BLOB";
    }
    throw new IOException("unknown SQL type: " + type);
  }

}
