package org.gora;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

public interface RowScanner<K, R extends TableRow>
extends Closeable {

  public Entry<K, R> next() throws IOException;
}
