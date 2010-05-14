package org.gora.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.serializer.Deserializer;
import org.gora.persistency.Persistent;
import org.gora.persistency.State;
import org.gora.persistency.StateManager;
import org.gora.persistency.StatefulHashMap;
import org.gora.persistency.StatefulMap;

public class PersistentDeserializer extends SpecificDatumReader
implements Deserializer<Persistent> {

  private BinaryDecoder decoder;
  private Class<Persistent> persistentClass;
  private boolean reuseObjects;

  public PersistentDeserializer(Class<Persistent> c, boolean reuseObjects) {
    this.persistentClass = c;
    this.reuseObjects = reuseObjects;
  }
  
  @Override
  public void open(InputStream in) throws IOException {
    decoder = new BinaryDecoder(in);
  }

  @Override
  public void close() throws IOException { }

  @Override
  public Persistent deserialize(Persistent persistent)
  throws IOException {
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
    setSchema(persistent.getSchema());

    List<Field> fields= persistent.getSchema().getFields();
    boolean[] isDirty = new boolean[fields.size()];

    int i = 0;
    for (Field field : fields) {
      boolean isReadable = decoder.readBoolean();
      isDirty[i++] = decoder.readBoolean();
      if (isReadable) {
        Object o = read(null, decoder);
        o = readExtraInformation(field.schema(), o, decoder);
        persistent.put(field.pos(), o);
      }
    }

    // Now set changed bits
    stateManager.clearDirty(persistent);
    for (i = 0; i < isDirty.length; i++) {
      if (isDirty[i]) {
        stateManager.setDirty(persistent, i);
      }
    }
    return persistent;
  }

  @SuppressWarnings("unchecked")
  private Object readExtraInformation(Schema schema, Object o, Decoder decoder)
  throws IOException {
    if (schema.getType() == Type.MAP) {
      StatefulMap<Utf8, ?> map = new StatefulHashMap((Map)o);
      map.clearStates();
      int size = decoder.readInt();
      for (int j = 0; j < size; j++) {
        Utf8 key = decoder.readString(null);
        State state = State.values()[decoder.readInt()];
        map.putState(key, state);
      }
      return map;
    }
    return o;
  }
}
