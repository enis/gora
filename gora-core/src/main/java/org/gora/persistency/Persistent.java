package org.gora.persistency;

import org.apache.avro.specific.SpecificRecord;

/**
 * Objects that are persisted by Gora implements this interface.
 */
public interface Persistent extends SpecificRecord {

  /**
   * Returns the StateManager which manages the persistent 
   * state of the object.
   * @return the StateManager of the object
   */
  public StateManager getStateManager();

  /**
   * Constructs a new instance of the object with the given StateManager.
   * This method is intended to be used by Gora framework.
   * @param stateManager the StateManager to manage the persistent state 
   * of the object
   * @return a new instance of the object
   */
  public Persistent newInstance(StateManager stateManager);

  /**
   * Returns whether the object is newly constructed.
   * @return true if the object is newly constructed, false if
   * retrieved from a datastore. 
   */
  public boolean isNew();
  
  /**
   * Sets the state of the object as new for persistency
   */
  public void setNew();
  
  /**
   * Clears the new state 
   */
  public void clearNew();
  
  /**
   * Returns whether any of the fields of the object has been modified 
   * after construction or loading. 
   * @return whether any of the fields of the object has changed
   */
  public boolean isDirty();
  
  /**
   * Returns whether the field has been modified.
   * @param fieldNum the offset of the field in the object
   * @return whether the field has been modified.
   */
  public boolean isDirty(int fieldNum);

  /**
   * Sets all the fields of the object as dirty.
   */
  public void setDirty();
  
  /**
   * Sets the field as dirty.
   * @param fieldNum the offset of the field in the object
   */
  public void setDirty(int fieldNum);
 
  /**
   * Clears the dirty state.
   */
  public void clearDirty();
  
  /**
   * Returns whether the field has been loaded from the datastore. 
   * @param fieldNum the offset of the field in the object
   * @return whether the field has been loaded 
   */
  public boolean isReadable(int fieldNum);

  /**
   * Sets the field as readable.
   * @param fieldNum the offset of the field in the object
   */
  public void setReadable(int fieldNum);

  /**
   * Clears the readable state.
   */
  public void clearReadable();
}
