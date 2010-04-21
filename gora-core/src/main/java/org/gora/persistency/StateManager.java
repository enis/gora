
package org.gora.persistency;

/**
 * StateManager manages objects state for persistency.
 */
public interface StateManager {

  /**
   * If one state manager is allocated per persistent object, 
   * call this method to set the managed persistent. 
   * @param persistent the persistent to manage
   */
  public void setManagedPersistent(Persistent persistent);

  /**
   * Returns whether the object is newly constructed.
   * @return true if the object is newly constructed, false if
   * retrieved from a datastore. 
   */
  public boolean isNew(Persistent persistent);
  
  /**
   * Sets the state of the object as new for persistency
   */
  public void setNew(Persistent persistent);
  
  /**
   * Clears the new state 
   */
  public void clearNew(Persistent persistent);

  /**
   * Returns whether any of the fields of the object has been modified 
   * after construction or loading. 
   * @return whether any of the fields of the object has changed
   */
  public boolean isDirty(Persistent persistent);
  
  /**
   * Returns whether the field has been modified.
   * @param fieldNum the offset of the field in the object
   * @return whether the field has been modified.
   */
  public boolean isDirty(Persistent persistent, int fieldNum);
  
  /**
   * Sets all the fields of the object as dirty.
   */
  public void setDirty(Persistent persistent);
  
  /**
   * Sets the field as dirty.
   * @param fieldNum the offset of the field in the object
   */
  public void setDirty(Persistent persistent, int fieldNum);

  /**
   * Clears the dirty state.
   */
  public void clearDirty(Persistent persistent);
  
  /**
   * Returns whether the field has been loaded from the datastore. 
   * @param fieldNum the offset of the field in the object
   * @return whether the field has been loaded 
   */
  public boolean isReadable(Persistent persistent, int fieldNum);
  
  /**
   * Sets the field as readable.
   * @param fieldNum the offset of the field in the object
   */
  public void setReadable(Persistent persistent, int fieldNum);

  /**
   * Clears the readable state.
   */
  public void clearReadable(Persistent persistent);
  
}
