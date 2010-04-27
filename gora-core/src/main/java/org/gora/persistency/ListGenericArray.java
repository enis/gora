
package org.gora.persistency;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;

/**
 * An {@link ArrayList} based implementation of Avro {@link GenericArray}.
 */
public class ListGenericArray<T> implements GenericArray<T> {

  private ArrayList<T> list;
  private Schema schema;
  
  public ListGenericArray(Schema schema) {
    this.schema = schema;
    this.list = new ArrayList<T>();
  }
  
  @Override
  public void add(T element) {
    list.add(element);
  }

  @Override
  public void clear() {
    list.clear();
  }

  @Override
  public T peek() {
    return null;
  }

  @Override
  public long size() {
    return list.size();
  }

  @Override
  public Iterator<T> iterator() {
    return list.iterator();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
