
package org.gora.sql.store;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Currency;
import java.util.HashMap;
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

  /**
   * Encapsules java.sql.Types as an enum
   */
  public static enum JdbcType { 
    ARRAY(Types.ARRAY),
    BIT(Types.BIT),
    BIGINT(Types.BIGINT),
    BINARY(Types.BINARY),
    BLOB(Types.BLOB),
    BOOLEAN(Types.BOOLEAN),
    CHAR(Types.CHAR),
    CLOB(Types.CLOB),
    DATALINK(Types.DATALINK),
    DATE(Types.DATE),
    DECIMAL(Types.DECIMAL),
    DISTINCT(Types.DISTINCT),
    DOUBLE(Types.DOUBLE),
    FLOAT(Types.FLOAT),
    INTEGER(Types.INTEGER),
    LONGNVARCHAR(Types.LONGNVARCHAR),
    LONGVARBINARY(Types.LONGVARBINARY),
    LONGVARCHAR(Types.LONGVARCHAR),
    NCHAR(Types.NCHAR),
    NCLOB(Types.NCLOB),
    NULL(Types.NULL),
    NUMERIC(Types.NUMERIC),
    NVARCHAR(Types.NVARCHAR),
    REAL(Types.REAL),
    REF(Types.REF),
    ROWID(Types.ROWID),
    SMALLINT(Types.SMALLINT),
    SQLXML(Types.SQLXML, "XML"),
    STRUCT(Types.STRUCT),
    TIME(Types.TIME),
    TIMESTAMP(Types.TIMESTAMP),
    TINYINT(Types.TINYINT),
    VARBINARY(Types.VARBINARY),
    VARCHAR(Types.VARCHAR)
    ;

    private int order;
    private String sqlType; 

    private JdbcType(int order) {
      this.order = order;
    }
    private JdbcType(int order, String sqlType) {
      this.order = order;
      this.sqlType = sqlType;
    }
    public String getSqlType() {
      return sqlType == null ? toString() : sqlType;
    }
    public int getOrder() {
      return order;
    }

    private static HashMap<Integer, JdbcType> map =
      new HashMap<Integer, JdbcType>();
    static {
      for(JdbcType type : JdbcType.values()) {
        map.put(type.order, type);
      }
    }

    /**
     * Returns a JdbcType enum from a jdbc type in java.sql.Types
     * @param order an integer in java.sql.Types
     */
    public static final JdbcType get(int order) {
      return map.get(order);
    }
  };

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

  public static JdbcType getJdbcType(Schema schema) throws IOException {
    Type type = schema.getType();

    switch(type) {
      case MAP    : return JdbcType.BLOB;
      case ARRAY  : return JdbcType.BLOB;
      case BOOLEAN: return JdbcType.BIT;
      case BYTES  : return JdbcType.BLOB;
      case DOUBLE : return JdbcType.DOUBLE;
      case ENUM   : return JdbcType.VARCHAR;        
      case FIXED  : return JdbcType.BINARY; 
      case FLOAT  : return JdbcType.FLOAT;
      case INT    : return JdbcType.INTEGER;  
      case LONG   : return JdbcType.BIGINT; 
      case NULL   : break;
      case RECORD : return JdbcType.BLOB;
      case STRING : return JdbcType.VARCHAR;        
      case UNION  : throw new IOException("Union is not supported yet");
    }
    return null;
  }

  public static JdbcType stringToJdbcType(String type) {
    return JdbcType.valueOf(type);
  }

}
