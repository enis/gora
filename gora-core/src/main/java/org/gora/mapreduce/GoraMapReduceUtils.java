
package org.gora.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.gora.util.StringUtils;

/**
 * MapReduce related utilities for Gora
 */
class GoraMapReduceUtils {

  static void setIOSerializations(Configuration conf, boolean reuseObjects) {
    String serializationClass =
      PersistentSerialization.class.getCanonicalName();
    if (!reuseObjects) {
      serializationClass =
        PersistentNonReusingSerialization.class.getCanonicalName();
    }
    String[] serializations = StringUtils.joinStringArrays(
        conf.getStrings("io.serializations"), 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        serializationClass); 
    conf.setStrings("io.serializations", serializations);
  }  
  
}
