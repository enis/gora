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
import org.gora.Persistent;
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
  public Persistent deserialize(Persistent row)
  throws IOException {
    if (row == null || !reuseOld) {
      try {
        row = rowClass.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      row.clearChangedBits();
      row.clearReadableBits();
    }
    setSchema(row.getSchema());

    Map<String, Field> fieldMap = row.getSchema().getFields();
    boolean[] changeds = new boolean[fieldMap.size()];

    int i = 0;
    for (Entry<String, Field> e : fieldMap.entrySet()) {
      boolean isReadable = decoder.readBoolean();
      changeds[i++] = decoder.readBoolean();
      Field field = e.getValue();
      if (isReadable) {
        Object o = read(null, field.schema(), field.schema(), decoder);
        o = readExtraInformation(field.schema(), o, decoder);
        row.set(field.pos(), o);
      }
    }

    // Now set changed bits
    row.clearChangedBits();
    for (i = 0; i < changeds.length; i++) {
      if (changeds[i]) {
        row.setFieldChanged(i);
      }
    }
    return row;
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
