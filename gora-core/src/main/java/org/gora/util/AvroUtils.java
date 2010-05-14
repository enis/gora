
package org.gora.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * An utility class for Avro related tasks 
 */
public class AvroUtils {

  /**
   * Returns a map of field name to Field for schema's fields.
   */
  public static Map<String, Field> getFieldMap(Schema schema) {
    List<Field> fields = schema.getFields();
    HashMap<String, Field> fieldMap = new HashMap<String, Field>(fields.size());
    for(Field field: fields) {
      fieldMap.put(field.name(), field);
    }
    return fieldMap;
  }
  
}
