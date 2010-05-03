
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
   * @param fieldIndex the offset of the field in the object
   * @return whether the field has been modified.
   */
  public boolean isDirty(Persistent persistent, int fieldIndex);
  
  /**
   * Sets all the fields of the object as dirty.
   */
  public void setDirty(Persistent persistent);
  
  /**
   * Sets the field as dirty.
   * @param fieldIndex the offset of the field in the object
   */
  public void setDirty(Persistent persistent, int fieldIndex);

  /**
   * Clears the field as dirty.
   * @param fieldIndex the offset of the field in the object
   */
  public void clearDirty(Persistent persistent, int fieldIndex);
  
  /**
   * Clears the dirty state.
   */
  public void clearDirty(Persistent persistent);
  
  /**
   * Returns whether the field has been loaded from the datastore. 
   * @param fieldIndex the offset of the field in the object
   * @return whether the field has been loaded 
   */
  public boolean isReadable(Persistent persistent, int fieldIndex);
  
  /**
   * Sets the field as readable.
   * @param fieldIndex the offset of the field in the object
   */
  public void setReadable(Persistent persistent, int fieldIndex);

  /**
   * Clears the field as readable.
   * @param fieldIndex the offset of the field in the object
   */
  public void clearReadable(Persistent persistent, int fieldIndex);
  
  /**
   * Clears the readable state.
   */
  public void clearReadable(Persistent persistent);
  
}
