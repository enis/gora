
package org.gora.store.impl;

import java.io.IOException;

import org.gora.persistency.Persistent;
import org.gora.store.DataStore;
import org.gora.util.ReflectionUtils;

public abstract class DataStoreBase<K, T extends Persistent> 
implements DataStore<K, T> {

  protected Class<K> keyClass;
  protected Class<T> persistentClass;
  
  public DataStoreBase() {
  }
  
  public DataStoreBase(Class<K> keyClass, Class<T> persistentClass) {
    this.keyClass = keyClass;
    this.persistentClass = persistentClass;
  }
  
  @Override
  public void setPersistentClass(Class<T> persistentClass) {
    this.persistentClass = persistentClass;
  }
  
  @Override
  public Class<T> getPersistentClass() {
    return persistentClass;
  }
  
  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }
  
  @Override
  public void setKeyClass(Class<K> keyClass) {
    if(keyClass != null)
      this.keyClass = keyClass;
  }
  
  @Override
  public T newInstance() throws IOException {
    try {
      return ReflectionUtils.newInstance(getPersistentClass());
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
}
