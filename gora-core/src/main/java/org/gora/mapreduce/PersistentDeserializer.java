package org.gora.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.serializer.Deserializer;
import org.gora.persistency.Persistent;
import org.gora.persistency.State;
import org.gora.persistency.StateManager;
import org.gora.persistency.StatefulHashMap;
import org.gora.persistency.StatefulMap;
import org.gora.util.IOUtils;

public class PersistentDeserializer 
  extends SpecificDatumReader<Persistent> implements Deserializer<Persistent> {

  private BinaryDecoder decoder;
  private Class<? extends Persistent> persistentClass;
  private boolean reuseObjects;
  private Persistent persistent;
  
  public PersistentDeserializer(Class<? extends Persistent> c, boolean reuseObjects) {
    this.persistentClass = c;
    this.reuseObjects = reuseObjects;
  }
  
  @Override
  public void open(InputStream in) throws IOException {
    /* It is very important to use a direct buffer, since Hadoop 
     * supplies an input stream that is only valid until the end of one 
     * record serialization. Each time deserialize() is called, the IS
     * is advanced to point to the right location, so we should not 
     * buffer the whole input stream at once.
     */
    decoder = new DecoderFactory().configureDirectDecoder(true)
      .createBinaryDecoder(in, decoder);
  }

  @Override
  public void close() throws IOException { }

  @Override
  public Persistent read(Persistent reuse, Decoder in) throws IOException {
    Schema schema = persistent.getSchema();
    return (Persistent) read(reuse, schema
        , new FakeResolvingDecoder(schema, in));
  }
  
  @Override
  public Persistent deserialize(Persistent persistent) throws IOException {
    
    StateManager stateManager = null;
    if (persistent == null || !reuseObjects) {
      try {
        persistent = persistentClass.newInstance();
        stateManager = persistent.getStateManager();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      stateManager = persistent.getStateManager();
      stateManager.clearDirty(persistent);
      stateManager.clearReadable(persistent);
    }
    this.persistent = persistent;
    setSchema(persistent.getSchema());    
    
    read(persistent, decoder);
    
    return persistent;
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {
    
    //check if top-level
    if(expected.equals(persistent.getSchema())) {
      boolean[] dirtyFields = IOUtils.readBoolArray(in);
      boolean[] readableFields = IOUtils.readBoolArray(in);

      //read fields
      int i = 0;
      Object record = newRecord(old, expected);
      
      for (Field f : expected.getFields()) {
        if(readableFields[f.pos()]) {
          int pos = f.pos();
          String name = f.name();
          Object oldDatum = (old != null) ? getField(record, name, pos) : null;
          setField(record, name, pos, read(oldDatum, f.schema(), in));
        }
      }
      
      // Now set changed bits
      StateManager stateManager = persistent.getStateManager();
      stateManager.clearDirty(persistent);
      for (i = 0; i < dirtyFields.length; i++) {
        if (dirtyFields[i]) {
          stateManager.setDirty(persistent, i);
        }
      }
      return record;
    } else {
      return super.readRecord(old, expected, in);
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected Object readMap(Object old, Schema expected, ResolvingDecoder in)
      throws IOException {
    
    StatefulMap<Utf8, ?> map = (StatefulMap<Utf8, ?>) newMap(old, 0);
    map.clearStates();
    int size = decoder.readInt();
    for (int j = 0; j < size; j++) {
      Utf8 key = decoder.readString(null);
      State state = State.values()[decoder.readInt()];
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
