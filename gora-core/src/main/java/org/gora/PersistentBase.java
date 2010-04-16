package org.gora;

import org.apache.avro.specific.SpecificRecord;

/**
 * Base classs implementing common functionality for Persistent
 * classes.
 */
public abstract class PersistentBase implements Persistent {
  
  private StateManager stateManager;
  
  protected PersistentBase() {
    this(new StateManagerImpl());
  }
  
  protected PersistentBase(StateManager stateManager) {
    System.out.println("const");
    this.stateManager = stateManager;
  }

  @Override
  public StateManager getStateManager() {
    return stateManager;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpecificRecord)) return false;

    SpecificRecord r2 = (SpecificRecord)o;
    if (!this.getSchema().equals(r2.getSchema())) return false;

    int end = this.getSchema().getFields().size();
    for (int i = 0; i < end; i++) {
      Object v1 = this.get(i);
      Object v2 = r2.get(i);
      if (v1 == null) {
        if (v2 != null) return false;
      } else {
        if (!v1.equals(v2)) return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    int end = this.getSchema().getFields().size();
    for (int i = 0; i < end; i++) {
      Object o = get(i);
      result = prime * result + ((o == null) ? 0 : o.hashCode());
    }
    return result;
  }
}
