
package org.gora.persistency;

public interface StateManager {

  public void setManagedPersistent(Persistent persistent);
  
  public void setDirty(Persistent persistent);
  
  public void setDirty(Persistent persistent, int fieldNum);

  public boolean isDirty(Persistent persistent);
  
  public boolean isDirty(Persistent persistent, int fieldNum);

  public void setReadable(Persistent persistent, int fieldNum);

  public boolean isReadable(Persistent persistent, int fieldNum);

  public void clearDirty(Persistent persistent);

  public void clearReadable(Persistent persistent);
  
}
