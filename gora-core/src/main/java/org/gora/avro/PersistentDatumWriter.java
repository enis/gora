
package org.gora.avro;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.gora.persistency.Persistent;
import org.gora.persistency.State;
import org.gora.persistency.StateManager;
import org.gora.persistency.StatefulMap;
import org.gora.util.IOUtils;

/**
 * PersistentDatumWriter writes, fields' dirty and readable information.
 */
public class PersistentDatumWriter<T extends Persistent> 
  extends SpecificDatumWriter<T> {

  private T persistent = null;
  
  public PersistentDatumWriter() {
  }
  
  public PersistentDatumWriter(Schema schema) {
    setSchema(schema);
  }
  
  public void setPersistent(T persistent) {
    this.persistent = persistent;
  }
  
  @Override
  /**exposing this function so that fields can be written individually*/
  public void write(Schema schema, Object datum, Encoder out)
      throws IOException {
    super.write(schema, datum, out);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected void writeRecord(Schema schema, Object datum, Encoder out)
      throws IOException {
    
    if(persistent == null) {
      persistent = (T) datum;
    }
    
    //check if top level schema
    if(schema.equals(persistent.getSchema())) {
      //write readable fields and dirty fields info
      boolean[] dirtyFields = new boolean[schema.getFields().size()];
      boolean[] readableFields = new boolean[schema.getFields().size()];
      StateManager manager = persistent.getStateManager();

      int i=0;
      for (@SuppressWarnings("unused") Field field : schema.getFields()) {
        dirtyFields[i] = manager.isDirty(persistent, i);
        readableFields[i] = manager.isReadable(persistent, i);
        i++;
      }

      IOUtils.writeBoolArray(out, dirtyFields);
      IOUtils.writeBoolArray(out, readableFields);

      for (Field field : schema.getFields()) {
        if(readableFields[field.pos()]) {
          write(field.schema(), getField(datum, field.name(), field.pos()), out);
        }
      }

    } else {
      super.writeRecord(schema, datum, out);
    }
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected void writeMap(Schema schema, Object datum, Encoder out)
      throws IOException {

    // write extra state information for maps
    StatefulMap<Utf8, ?> map = (StatefulMap) datum;
    out.writeInt(map.states().size());
    for (Entry<Utf8, State> e2 : map.states().entrySet()) {
      out.writeString(e2.getKey());
      out.writeInt(e2.getValue().ordinal());
    }
    
    super.writeMap(schema, datum, out);
  }
  
}
