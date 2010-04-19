package org.gora.persistency;

import org.apache.avro.specific.SpecificRecord;

/**
 * Objects that are persisted by Gora implements this interface.
 */
public interface Persistent extends SpecificRecord {

  /**
   * Returns the StateManager which manages the persistent 
   * state of the object.
   * @return
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
  
}
