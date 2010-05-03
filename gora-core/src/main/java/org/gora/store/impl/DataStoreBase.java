
package org.gora.store.impl;

import java.io.IOException;
import java.util.Properties;

import org.gora.persistency.BeanFactory;
import org.gora.persistency.Persistent;
import org.gora.persistency.impl.BeanFactoryImpl;
import org.gora.store.DataStore;

public abstract class DataStoreBase<K, T extends Persistent> 
implements DataStore<K, T> {

  protected BeanFactory<K, T> beanFactory;
  
  protected Class<K> keyClass;
  protected Class<T> persistentClass;
  
  public DataStoreBase() {
  }
  
  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException {
    setKeyClass(keyClass);
    setPersistentClass(persistentClass);
    if(this.beanFactory == null)
      this.beanFactory = new BeanFactoryImpl<K, T>(keyClass, persistentClass);
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
  public K newKey() throws IOException {
    try {
      return beanFactory.newKey();
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public T newPersistent() throws IOException {
    try {
      return beanFactory.newPersistent();
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
  
  @Override
  public void setBeanFactory(BeanFactory<K, T> beanFactory) {
    this.beanFactory = beanFactory;
  }
  
  @Override
  public BeanFactory<K, T> getBeanFactory() {
    return beanFactory;
  }
  
  /**
   * Checks whether the fields argument is null, and if so 
   * returns all the fields of the Persistent object, else returns the 
   * argument.
   */
  protected String[] getFieldsToQuery(String[] fields) {
    if(fields != null) {
      return fields;
    }
    return beanFactory.getCachedPersistent().getFields();
  }
  
}
