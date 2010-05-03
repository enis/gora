
package org.gora.persistency.impl;

import java.lang.reflect.Constructor;

import org.gora.persistency.BeanFactory;
import org.gora.persistency.Persistent;
import org.gora.util.ReflectionUtils;

/**
 * A default implementation of the {@link BeanFactory} interface. Constructs 
 * the keys using by reflection, {@link Persistent} objects by calling 
 * {@link Persistent#newInstance(org.gora.persistency.StateManager)}. 
 */
public class BeanFactoryImpl<K, T extends Persistent> implements BeanFactory<K, T> {

  private Class<K> keyClass;
  private Class<T> persistentClass;
  
  private Constructor<K> keyConstructor;
  
  private T persistent;
  
  public BeanFactoryImpl(Class<K> keyClass, Class<T> persistentClass) {
    this.keyClass = keyClass;
    this.persistentClass = persistentClass;
    
    try {
      this.keyConstructor = ReflectionUtils.getConstructor(keyClass);
      this.persistent = ReflectionUtils.newInstance(persistentClass);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  public K newKey() throws Exception {
    return keyConstructor.newInstance(ReflectionUtils.EMPTY_OBJECT_ARRAY);
  }
 
  @SuppressWarnings("unchecked")
  @Override
  public T newPersistent() {
    return (T) persistent.newInstance(new StateManagerImpl());
  }
  
  @Override
  public T getCachedPersistent() {
    return persistent;
  }
  
  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }
  
  @Override
  public Class<T> getPersistentClass() {
    return persistentClass;
  }
  
}
