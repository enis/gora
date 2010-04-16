package org.gora;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

public interface RowScanner<K, T extends Persistent> extends Closeable {

  public Entry<K, T> next() throws IOException;
  
}
