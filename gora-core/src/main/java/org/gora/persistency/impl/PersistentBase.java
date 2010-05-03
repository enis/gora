package org.gora.persistency.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;

/**
 * Base classs implementing common functionality for Persistent
 * classes.
 */
public abstract class PersistentBase implements Persistent {
  
  protected static Map<String, Integer> FIELD_MAP;
  
  protected static String[] FIELDS = null;
  
  private StateManager stateManager;
  
  protected PersistentBase() {
    this(new StateManagerImpl());
  }
  
  protected PersistentBase(StateManager stateManager) {
    this.stateManager = stateManager;
    stateManager.setManagedPersistent(this);
  }

  /** Subclasses should call this function for all the persistable fields 
   * in the class to register them.
   * @param field the name of the field
   * @param index the index of the field
   */
  protected static void registerFields(String... fields) {
    FIELDS = fields;
    FIELD_MAP = new HashMap<String, Integer>(FIELDS.length);
    
    for(int i=0; i < FIELDS.length; i++) {
      FIELD_MAP.put(fields[i], i);
    }
  }
  
  @Override
  public StateManager getStateManager() {
    return stateManager;
  }
  
  @Override
  public String[] getFields() {
    return FIELDS;
  }
  
  @Override
  public String getField(int index) {
    return FIELDS[index];
  }
  
  @Override
  public int getFieldIndex(String field) {
    return FIELD_MAP.get(field);
  }
  
  @Override
  public boolean isNew() {
    return getStateManager().isNew(this);
  }
  
  @Override
  public void setNew() {
    getStateManager().setNew(this);
  }
  
  @Override
  public void clearNew() {
    getStateManager().clearNew(this);
  }
  
  @Override
  public boolean isDirty() {
    return getStateManager().isDirty(this);
  }
  
  @Override
  public boolean isDirty(int fieldIndex) {
    return getStateManager().isDirty(this, fieldIndex);
  }
  
  @Override
  public boolean isDirty(String field) {
    return isDirty(getFieldIndex(field));
  }
  
  @Override
  public void setDirty() {
    getStateManager().setDirty(this);
  }
  
  @Override
  public void setDirty(int fieldIndex) {
    getStateManager().setDirty(this, fieldIndex);
  }
  
  @Override
  public void setDirty(String field) {
    setDirty(getFieldIndex(field));
  }
  
  @Override
  public void clearDirty(int fieldIndex) {
    getStateManager().clearDirty(this, fieldIndex);
  }
  
  @Override
  public void clearDirty(String field) {
    clearDirty(getFieldIndex(field));
  }
  
  @Override
  public void clearDirty() {
    getStateManager().clearDirty(this);
  }
  
  @Override
  public boolean isReadable(int fieldIndex) {
    return getStateManager().isReadable(this, fieldIndex);
  }
  
  @Override
  public boolean isReadable(String field) {
    return isReadable(getFieldIndex(field));
  }
  
  @Override
  public void setReadable(int fieldIndex) {
    getStateManager().setReadable(this, fieldIndex);
  }
  
  @Override
  public void setReadable(String field) {
    setReadable(getFieldIndex(field));
  }
  
  @Override
  public void clearReadable() {
    getStateManager().clearReadable(this);
  }
  
  @Override
  public void clearReadable(int fieldIndex) {
    getStateManager().clearReadable(this, fieldIndex);
  }
  
  @Override
  public void clearReadable(String field) {
    clearReadable(getFieldIndex(field));
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
