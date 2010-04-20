
package org.gora.mock.persistency;

import org.apache.avro.Schema;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;
import org.gora.persistency.impl.PersistentBase;

public class MockPersistent extends PersistentBase {

  @Override
  public Persistent newInstance(StateManager stateManager) {
    return new MockPersistent();
  }

  @Override
  public Object get(int arg0) {
    return null;
  }

  @Override
  public void set(int arg0, Object arg1) {
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
