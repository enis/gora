package org.gora.store;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.gora.TableRow;

public class TableSerializerFactory {
  public static final String SERIALIZER_KEY = "gora.serializer.class";
  
  @SuppressWarnings("unchecked")
  public static <K, R extends TableRow> TableSerializer<K, R>
  create(Configuration conf, Class<K> keyClass, Class<R> rowClass) {
    String className =
      conf.get(SERIALIZER_KEY, "org.gora.store.hbase.HbaseSerializer");
    Class<? extends TableSerializer<K, R>> clazz;
    try {
      clazz = (Class<? extends TableSerializer<K, R>>)
        TableSerializerFactory.class.getClassLoader().loadClass(className);
      Constructor<? extends TableSerializer<K, R>> constructor =
        clazz.getConstructor(Configuration.class, Class.class, Class.class);
      return constructor.newInstance(conf, keyClass, rowClass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
