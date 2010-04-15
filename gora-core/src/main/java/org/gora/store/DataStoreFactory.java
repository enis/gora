package org.gora.store;

import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.gora.Persistent;

public class DataStoreFactory {
  public static final String SERIALIZER_KEY = "gora.serializer.class";
  
  @SuppressWarnings("unchecked")
  public static <K, R extends Persistent> DataStore<K, R>
  create(Configuration conf, Class<K> keyClass, Class<R> rowClass) {
    String className =
      conf.get(SERIALIZER_KEY, "org.gora.store.hbase.HbaseSerializer");
    Class<? extends DataStore<K, R>> clazz;
    try {
      clazz = (Class<? extends DataStore<K, R>>)
        DataStoreFactory.class.getClassLoader().loadClass(className);
      Constructor<? extends DataStore<K, R>> constructor =
        clazz.getConstructor(Configuration.class, Class.class, Class.class);
      return constructor.newInstance(conf, keyClass, rowClass);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
