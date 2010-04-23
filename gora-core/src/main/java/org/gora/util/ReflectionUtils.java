
package org.gora.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utility methods related to reflection
 */
public class ReflectionUtils {

  /**
   * Constructs a new instance of the class using the no-arg constructor.
   * @param clazz the class of the object
   * @return a new instance of the object
   */
  public static <T> T newInstance(Class<T> clazz) throws InstantiationException
  , IllegalAccessException, SecurityException, NoSuchMethodException
  , IllegalArgumentException, InvocationTargetException {
    if(clazz == null) {
      throw new IllegalArgumentException("class cannot be null");
    }
    Constructor<T> cons = clazz.getConstructor();
    cons.setAccessible(true);
    return cons.newInstance();
  }
  
  /**
   * Constructs a new instance of the class using the no-arg constructor.
   * @param classStr the class name of the object
   * @return a new instance of the object
   */
  public static Object newInstance(String classStr) throws InstantiationException
    , IllegalAccessException, ClassNotFoundException, SecurityException
    , IllegalArgumentException, NoSuchMethodException, InvocationTargetException {
    if(classStr == null) {
      throw new IllegalArgumentException("class cannot be null");
    }
    Class<?> clazz = Class.forName(classStr);
    return newInstance(clazz);
  }
  
}
