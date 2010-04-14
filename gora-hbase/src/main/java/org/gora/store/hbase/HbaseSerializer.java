package org.gora.store.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.gora.RowScanner;
import org.gora.TableRow;
import org.gora.store.TableSerializer;
import org.gora.util.NodeWalker;
import org.gora.util.StatefulHashMap;
import org.gora.util.XmlUtils;
import org.gora.util.StatefulHashMap.State;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class HbaseSerializer<K, R extends TableRow>
extends TableSerializer<K, R> {

  public static final String PARSE_MAPPING_FILE_KEY = "gora.hbase.mapping.file"; 

  public static final String DEFAULT_FILE_NAME = "hbase-mapping.xml";

  private static final DocumentBuilder docBuilder;

  // a map from field name to hbase column
  private Map<String, HbaseColumn> columnMap;

  private List<HColumnDescriptor> colDescs;

  private String tableName;

  private HTable table;

  private Schema schema;

  static {
    try {
      docBuilder = 
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    } catch (FactoryConfigurationError e) {
      throw new RuntimeException(e);
    }
  }

  private class HbaseScanner implements RowScanner<K, R> {
    private final ResultScanner scanner;

    private final String[] fields;

    private HbaseScanner(ResultScanner scanner, String[] fields) {
      this.scanner = scanner;
      this.fields = fields;
    }

    @Override
    public Entry<K, R> next() throws IOException {
      Result result = scanner.next();
      if (result == null) {
        return null;
      }
      K key = HbaseSerializer.this.fromBytes(getKeyClass(), result.getRow());
      R row = makeTableRow(result, fields);
      return new SimpleEntry<K, R>(key, row);
    }

    @Override
    public void close() throws IOException {
      scanner.close();
    }
  }

  public HbaseSerializer(Configuration conf, Class<K> keyClass, Class<R> rowClass)  {
    super(conf, keyClass, rowClass);
    columnMap = new HashMap<String, HbaseColumn>();
    colDescs = new ArrayList<HColumnDescriptor>();
    try {
      parseMapping(getConf().get(PARSE_MAPPING_FILE_KEY, DEFAULT_FILE_NAME));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createTable() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration(getConf()));
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    for (HColumnDescriptor colDesc : colDescs) {
      tableDesc.addFamily(colDesc);
    }
    admin.createTable(tableDesc);
    table = new HTable(tableName);
  }

  @Override
  public R makeRow() throws IOException {
    try {
      R row = getRowClass().newInstance();
      return row;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public R readRow(K key, String[] fields) throws IOException {
    Get get = new Get(toBytes(key));
    addFields(get, fields);
    Result result = table.get(get);
    return makeTableRow(result, fields);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void updateRow(K key, R row) throws IOException {
    Schema schema = row.getSchema();
    byte[] keyRaw = toBytes(key);
    Put put = new Put(keyRaw);
    Delete delete = new Delete(keyRaw);
    boolean hasPuts = false;
    boolean hasDeletes = false;
    Iterator<Entry<String, Schema>> iter =
      schema.getFieldSchemas().iterator();
    for (int i = 0; iter.hasNext(); i++) {
      Entry<String, Schema> field = iter.next();
      if (!row.isFieldChanged(i)) {
        continue;
      }
      Type type = field.getValue().getType();
      Object o = row.get(i);
      HbaseColumn hcol = columnMap.get(field.getKey());
      if (type == Type.MAP) {
        StatefulHashMap<Utf8, ?> map = (StatefulHashMap<Utf8, ?>) o;
        for (Entry<Utf8, State> e : map.states().entrySet()) {
          Utf8 mapKey = e.getKey();
          switch (e.getValue()) {
          case UPDATED:
            byte[] qual = Bytes.toBytes(mapKey.toString());
            byte[] val = toBytes(map.get(mapKey), field.getValue().getValueType());
            put.add(hcol.getFamily(), qual, val);
            hasPuts = true;
            break;
          case DELETED:
            qual = Bytes.toBytes(mapKey.toString());
            hasDeletes = true;
            delete.deleteColumn(hcol.getFamily(), qual);
            break;
          }
        }
      } else {
        put.add(hcol.getFamily(), hcol.getQualifier(), toBytes(o, field.getValue()));
        hasPuts = true;
      }
    }
    if (hasPuts) {
      table.put(put);
    }
    if (hasDeletes) {
      table.delete(delete);
    }
  }

  @Override
  public void deleteRow(K key) throws IOException {
    table.delete(new Delete(toBytes(key)));
  }

  @Override
  public void sync() throws IOException {
    table.flushCommits();
  }

  @Override
  public List<InputSplit> getSplits(K start, K stop, JobContext context)
  throws IOException {
    // taken from o.a.h.hbase.mapreduce.TableInputFormatBase
    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
    if (keys == null || keys.getFirst() == null || 
        keys.getFirst().length == 0) {
      throw new IOException("Expecting at least one region.");
    }
    if (table == null) {
      throw new IOException("No table was provided.");
    }
    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length); 
    for (int i = 0; i < keys.getFirst().length; i++) {
      String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
      getServerAddress().getHostname();
      byte[] startRow = start != null ? toBytes(start) : new byte[0];
      byte[] stopRow = stop != null ? toBytes(stop) : new byte[0];
      // determine if the given start an stop key fall into the region
      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
          Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
          (stopRow.length == 0 || 
              Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
        byte[] splitStart = startRow.length == 0 || 
        Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? 
            keys.getFirst()[i] : startRow;
            byte[] splitStop = stopRow.length == 0 || 
            Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0 ? 
                keys.getSecond()[i] : stopRow;
                InputSplit split = new TableSplit(table.getTableName(),
                    splitStart, splitStop, regionLocation);
                splits.add(split);
      }
    }
    return splits;
  }

  @Override
  public RowScanner<K, R> makeScanner(K startRow, K stopRow, final String[] fields)
  throws IOException {
    final Scan scan = new Scan();
    if (startRow != null) {
      scan.setStartRow(toBytes(startRow));
    }
    if (stopRow != null) {
      scan.setStopRow(toBytes(stopRow));
    }
    addFields(scan, fields);
    final ResultScanner scanner = table.getScanner(scan);
    return new HbaseScanner(scanner, fields);
  }

  @Override
  public RowScanner<K, R> makeScanner(InputSplit split, String[] fields)
  throws IOException {
    TableSplit tSplit = (TableSplit) split;
    K startRow = fromBytes(getKeyClass(), tSplit.getStartRow());
    K endRow = fromBytes(getKeyClass(), tSplit.getEndRow());
    return makeScanner(startRow, endRow, fields);
  }

  private void addFields(Get get, String[] fields) {
    Map<String, Field> fieldMap = schema.getFields();
    for (String f : fields) {
      HbaseColumn col = columnMap.get(f);
      Schema fieldSchema = fieldMap.get(f).schema();
      if (fieldSchema.getType() == Type.MAP) {
        get.addFamily(col.family);
      } else {
        get.addColumn(col.family, col.qualifier);
      }
    }
  }

  private void addFields(Scan scan, String[] fields)
  throws IOException {
    Map<String, Field> fieldMap = schema.getFields();
    for (String f : fields) {
      HbaseColumn col = columnMap.get(f);
      Schema fieldSchema = fieldMap.get(f).schema();
      if (fieldSchema.getType() == Type.MAP) {
        scan.addFamily(col.family);
      } else {
        scan.addColumn(col.family, col.qualifier);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private R makeTableRow(Result result, String[] fields)
  throws IOException {
    R row = makeRow();
    Schema schema = row.getSchema();
    Map<String, Field> fieldMap = schema.getFields();
    for (String f : fields) {
      HbaseColumn col = columnMap.get(f);
      Field field = fieldMap.get(f);
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Type.MAP) {
        NavigableMap<byte[], byte[]> qualMap =
          result.getNoVersionMap().get(col.getFamily());
        if (qualMap == null) {
          continue;
        }
        Schema valueSchema = fieldSchema.getValueType();
        Map map = new HashMap();
        for (Entry<byte[], byte[]> e : qualMap.entrySet()) {
          map.put(new Utf8(Bytes.toString(e.getKey())), 
              fromBytes(valueSchema, e.getValue()));
        }
        setField(row, field, map);
      } else {
        byte[] val =
          result.getValue(col.getFamily(), col.getQualifier());
        if (val == null) {
          continue;
        }
        setField(row, field, val);
      }
    }
    row.clearChangedBits();
    return row;
  }

  @SuppressWarnings("unchecked")
  private Object fromBytes(Schema schema, byte[] val) {
    Type type = schema.getType();
    switch (type) {
    case ENUM:
      String symbol = schema.getEnumSymbols().get(val[0]);
      return Enum.valueOf(ReflectData.get().getClass(schema), symbol);
    case STRING:  return new Utf8(Bytes.toString(val));
    case BYTES:   return ByteBuffer.wrap(val);
    case INT:     return Bytes.toInt(val);
    case LONG:    return Bytes.toLong(val);
    case FLOAT:   return Bytes.toFloat(val);
    case DOUBLE:  return Bytes.toDouble(val);
    case BOOLEAN: return val[0] != 0;
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }

  @SuppressWarnings("unchecked")
  private K fromBytes(Class<K> clazz, byte[] val) {
    if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return (K) Byte.valueOf(val[0]);
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return (K) Boolean.valueOf(val[0] == 0 ? false : true);
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return (K) Short.valueOf(Bytes.toShort(val));
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return (K) Integer.valueOf(Bytes.toInt(val));
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return (K) Long.valueOf(Bytes.toLong(val));
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return (K) Float.valueOf(Bytes.toFloat(val));
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return (K) Double.valueOf(Bytes.toDouble(val));
    } else if (clazz.equals(String.class)) {
      return (K) Bytes.toString(val);
    } else if (clazz.equals(Utf8.class)) {
      return (K) new Utf8(Bytes.toString(val));
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  private byte[] toBytes(Object o) {
    Class<?> clazz = o.getClass();
    if (clazz.equals(Enum.class)) {
      return new byte[] { (byte)((Enum<?>) o).ordinal() }; // yeah, yeah it's a hack
    } else if (clazz.equals(Byte.TYPE) || clazz.equals(Byte.class)) {
      return new byte[] { (Byte) o };
    } else if (clazz.equals(Boolean.TYPE) || clazz.equals(Boolean.class)) {
      return new byte[] { ((Boolean) o ? (byte) 1 :(byte) 0)};
    } else if (clazz.equals(Short.TYPE) || clazz.equals(Short.class)) {
      return Bytes.toBytes((Short) o);
    } else if (clazz.equals(Integer.TYPE) || clazz.equals(Integer.class)) {
      return Bytes.toBytes((Integer) o);
    } else if (clazz.equals(Long.TYPE) || clazz.equals(Long.class)) {
      return Bytes.toBytes((Long) o);
    } else if (clazz.equals(Float.TYPE) || clazz.equals(Float.class)) {
      return Bytes.toBytes((Float) o);
    } else if (clazz.equals(Double.TYPE) || clazz.equals(Double.class)) {
      return Bytes.toBytes((Double) o);
    } else if (clazz.equals(String.class)) {
      return Bytes.toBytes((String) o);
    } else if (clazz.equals(Utf8.class)) {
      return ((Utf8) o).getBytes();
    }
    throw new RuntimeException("Can't parse data as class: " + clazz);
  }

  private byte[] toBytes(Object o, Schema schema) {
    Type type = schema.getType();
    switch (type) {
    case STRING:  return Bytes.toBytes(((Utf8)o).toString()); // TODO: maybe ((Utf8)o).getBytes(); ?
    case BYTES:   return ((ByteBuffer)o).array();
    case INT:     return Bytes.toBytes((Integer)o);
    case LONG:    return Bytes.toBytes((Long)o);
    case FLOAT:   return Bytes.toBytes((Float)o);
    case DOUBLE:  return Bytes.toBytes((Double)o);
    case BOOLEAN: return (Boolean)o ? new byte[] {1} : new byte[] {0};
    case ENUM:    return new byte[] { (byte)((Enum<?>) o).ordinal() };
    default: throw new RuntimeException("Unknown type: "+type);
    }
  }

  @SuppressWarnings("unchecked")
  private void setField(R row, Field field, Map map) {
    row.set(field.pos(), new StatefulHashMap(map));
  }

  private void setField(R row, Field field, byte[] val) {
    row.set(field.pos(), fromBytes(field.schema(), val));
  }

  @SuppressWarnings("unchecked")
  private void parseMapping(String fileName)
  throws ClassNotFoundException, InstantiationException, IllegalAccessException,
  SecurityException, NoSuchFieldException {
    try {      
      InputStream stream =
        HbaseSerializer.class.getClassLoader().getResourceAsStream(fileName);
      Document doc = docBuilder.parse(stream);
      NodeWalker walker = new NodeWalker(doc.getFirstChild());
      boolean processInfo = false;

      while (walker.hasNext()) {
        Node node = walker.nextNode();
        if (node.getNodeType() == Node.TEXT_NODE) {
          continue;
        }
        if (node.getNodeName().equals("table")) {
          Class<K> currentKeyClass =
            (Class<K>) Class.forName(XmlUtils.getAttribute(node, "keyClass"));
          Class<R> currentRowClass =
            (Class<R>) Class.forName(XmlUtils.getAttribute(node, "rowClass"));
          if (!currentKeyClass.equals(getKeyClass())) {
            processInfo = false;
            continue;
          }
          if (!currentRowClass.equals(getRowClass())) {
            processInfo = false;
            continue;
          }

          tableName = XmlUtils.getAttribute(node, "name");
          HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration(getConf()));
          if (admin.tableExists(tableName)) {
            table = new HTable(tableName);
            table.setAutoFlush(false);
          } else {
            table = null;
          }
          schema = getRowClass().newInstance().getSchema();
          processInfo = true;
        } else if (node.getNodeName().equals("field") && processInfo) {
          String fieldName = XmlUtils.getAttribute(node, "name");
          String familyStr = XmlUtils.getAttribute(node, "family");
          String qualifierStr = XmlUtils.getAttribute(node, "qualifier");
          byte[] family = Bytes.toBytes(familyStr);
          byte[] qualifier =
            qualifierStr != null ? Bytes.toBytes(qualifierStr) : null;
            columnMap.put(fieldName, new HbaseColumn(family, qualifier));
        } else if (node.getNodeName().equals("family") && processInfo) {
          String familyName = XmlUtils.getAttribute(node, "name");
          String compression =
            XmlUtils.getAttribute(node, "compression",
                HColumnDescriptor.DEFAULT_COMPRESSION.toUpperCase());
          HColumnDescriptor colDesc = new HColumnDescriptor(familyName);
          colDesc.setCompressionType(Algorithm.valueOf(compression));
          colDescs.add(new HColumnDescriptor(familyName));
        }
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    sync();
    if(table != null) 
      table.close();
  }

}
