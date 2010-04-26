
package org.gora.util;

import java.util.HashSet;

/**
 * An utility class for String related functionality. 
 */
public class StringUtils {

  /**
   * Joins the two given arrays, removing dup elements. 
   */
  public static String[] joinStringArrays(String[] arr1, String... arr2) {
    HashSet<String> set = new HashSet<String>();
    for(String str : arr1) set.add(str);
    for(String str : arr2) set.add(str);
    
    return set.toArray(new String[set.size()]);
  }
  
}
