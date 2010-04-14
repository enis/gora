package org.gora.mapreduce;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.gora.TableRow;

public class TableRowSerialization
implements Serialization<TableRow> {

  @Override
  public boolean accept(Class<?> c) {
    return TableRow.class.isAssignableFrom(c);
  }

  @Override
  public Deserializer<TableRow> getDeserializer(Class<TableRow> c) {
    return new TableRowDeserializer(c, true);
  }

  @Override
  public Serializer<TableRow> getSerializer(Class<TableRow> c) {
    return new TableRowSerializer();
  }
}
