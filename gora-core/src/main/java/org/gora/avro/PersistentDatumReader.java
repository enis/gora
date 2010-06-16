
package org.gora.avro;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.gora.mapreduce.FakeResolvingDecoder;
import org.gora.persistency.Persistent;
import org.gora.persistency.State;
import org.gora.persistency.StatefulHashMap;
import org.gora.persistency.StatefulMap;
import org.gora.persistency.impl.StateManagerImpl;
import org.gora.util.IOUtils;

/**
 * PersistentDatumReader reads, fields' dirty and readable information.
 */
public class PersistentDatumReader<T extends Persistent> 
  extends SpecificDatumReader<T> {

  private Schema rootSchema;
  private T cachedPersistent; // for creating objects
  
  private WeakHashMap<Decoder, ResolvingDecoder> decoderCache 
    = new WeakHashMap<Decoder, ResolvingDecoder>();
  
  public PersistentDatumReader() {
  }
  
  public PersistentDatumReader(Schema schema) {
    setSchema(schema);
  }
  
  @Override
  public void setSchema(Schema actual) {
    this.rootSchema = actual;
    super.setSchema(actual);
  }
  
  @SuppressWarnings("unchecked")
  public T newPersistent() {
    if(cachedPersistent == null) {
      cachedPersistent = (T)super.newRecord(null, rootSchema);
      return cachedPersistent; //we can return the cached object
    }
    return (T)cachedPersistent.newInstance(new StateManagerImpl());
  }
  
  @Override
  protected Object newRecord(Object old, Schema schema) {
    if(old != null) {
      return old;
    }
    
    if(schema.equals(rootSchema)) {
      return newPersistent();
    } else {
      return super.newRecord(old, schema);
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public T read(T reuse, Decoder in) throws IOException {
    return (T) read(reuse, rootSchema, in);
  }
  
  public Object read(Object reuse, Schema schema, Decoder decoder) 
    throws IOException {
    return super.read(reuse, schema, getResolvingDecoder(decoder));
  }
  
  protected ResolvingDecoder getResolvingDecoder(Decoder decoder) 
  throws IOException {
    ResolvingDecoder resolvingDecoder = decoderCache.get(decoder);
    if(resolvingDecoder == null) {
      resolvingDecoder = new FakeResolvingDecoder(rootSchema, decoder);
      decoderCache.put(decoder, resolvingDecoder);
    }
    return resolvingDecoder;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {
    
    Object record = newRecord(old, expected);
    
    //check if top-level
    if(expected.equals(rootSchema)) {
      T persistent = (T)record;
      persistent.clear();
      
      boolean[] dirtyFields = IOUtils.readBoolArray(in);
      boolean[] readableFields = IOUtils.readBoolArray(in);

      //read fields
      int i = 0;
      
      for (Field f : expected.getFields()) {
        if(readableFields[f.pos()]) {
          int pos = f.pos();
          String name = f.name();
          Object oldDatum = (old != null) ? getField(record, name, pos) : null;
          setField(record, name, pos, read(oldDatum, f.schema(), in));
        }
      }
      
      // Now set changed bits
      for (i = 0; i < dirtyFields.length; i++) {
        if (dirtyFields[i]) {
          persistent.setDirty(i);
        }
      }
      return record;
    } else {
      //since ResolvingDecoder.readFieldOrder is final, we cannot override it
      //so this is a copy of super.readReacord, with the readFieldOrder change
      
      for (Field f : expected.getFields()) {
        int pos = f.pos();
        String name = f.name();
        Object oldDatum = (old != null) ? getField(record, name, pos) : null;
        setField(record, name, pos, read(oldDatum, f.schema(), in));
      }

      return record;
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected Object readMap(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {
    
    StatefulMap<Utf8, ?> map = (StatefulMap<Utf8, ?>) newMap(old, 0);
    map.clearStates();
    int size = in.readInt();
    for (int j = 0; j < size; j++) {
      Utf8 key = in.readString(null);
      State state = State.values()[in.readInt()];
      map.putState(key, state);
    }
    
    return super.readMap(map, expected, in);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected Object newMap(Object old, int size) {
    if (old instanceof StatefulHashMap) {
      ((Map) old).clear();
      ((StatefulHashMap)old).clearStates();
      return old;
    }
    return new StatefulHashMap<Object, Object>();
  }
  
}
