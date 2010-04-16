
package org.gora;

import java.util.BitSet;

/**
 * An implementation for the StateManager. This implementation assumes 
 * every Persistent object has it's own StateManager.
 */
public class StateManagerImpl implements StateManager {

  protected BitSet dirtyBits;
  protected BitSet readableBits;

  public StateManagerImpl() {
  }

  public void setManagedPersistent(Persistent persistent) {
    System.out.println("setManagedPersitent");
    dirtyBits = new BitSet(persistent.getSchema().getFields().size());
    readableBits = new BitSet(persistent.getSchema().getFields().size());    
  }
  
  public void setDirty(Persistent persistent, int fieldNum) {
    dirtyBits.set(fieldNum);
    readableBits.set(fieldNum);
  }
  
  public boolean isDirty(Persistent persistent, int fieldNum) {
    return dirtyBits.get(fieldNum);
  }

  public boolean isDirty(Persistent persistent) {
    throw new RuntimeException("not yet impl.");
  }
  
  public void setReadable(Persistent persistent, int fieldNum) {
    readableBits.set(fieldNum);
  }

  public boolean isReadable(Persistent persistent, int fieldNum) {
    return readableBits.get(fieldNum);
  }

  public void clearDirty(Persistent persistent) {
    dirtyBits.clear();
  }

  public void clearReadable(Persistent persistent) {
    readableBits.clear();
  }

  @Override
  public void setDirty(Persistent persistent) {
    throw new RuntimeException("not yet impl.");
  }
}
