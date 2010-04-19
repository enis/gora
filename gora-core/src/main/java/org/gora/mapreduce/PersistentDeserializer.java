package org.gora.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.serializer.Deserializer;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;
import org.gora.util.StatefulHashMap;
import org.gora.util.StatefulHashMap.State;

public class PersistentDeserializer extends SpecificDatumReader
implements Deserializer<Persistent> {

  private BinaryDecoder decoder;
  private Class<Persistent> rowClass;
  private boolean reuseOld;

  public PersistentDeserializer(Class<Persistent> c, boolean reuseOld) {
    this.rowClass = c;
    this.reuseOld = reuseOld;
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
    if (persistent == null || !reuseOld) {
      try {
        persistent = rowClass.newInstance();
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

    Map<String, Field> fieldMap = persistent.getSchema().getFields();
    boolean[] isDirty = new boolean[fieldMap.size()];

    int i = 0;
    for (Entry<String, Field> e : fieldMap.entrySet()) {
      boolean isReadable = decoder.readBoolean();
      isDirty[i++] = decoder.readBoolean();
      Field field = e.getValue();
      if (isReadable) {
        Object o = read(null, field.schema(), field.schema(), decoder);
        o = readExtraInformation(field.schema(), o, decoder);
        persistent.set(field.pos(), o);
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
      StatefulHashMap<Utf8, ?> map = new StatefulHashMap((Map)o);
      map.resetStates();
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
