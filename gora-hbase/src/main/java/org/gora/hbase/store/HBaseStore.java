package org.gora.hbase.store;

import static org.gora.hbase.util.HBaseByteInterface.fromBytes;
import static org.gora.hbase.util.HBaseByteInterface.toBytes;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.gora.hbase.query.HBaseQuery;
import org.gora.hbase.query.HBaseScannerResult;
import org.gora.hbase.util.HBaseByteInterface;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.impl.PartitionQueryImpl;
import org.gora.store.impl.DataStoreBase;
import org.gora.util.NodeWalker;
import org.gora.util.StatefulHashMap;
import org.gora.util.XmlUtils;
import org.gora.util.StatefulHashMap.State;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * DataStore for HBase. 
 * 
 * <p> Note: HBaseStore is not yet thread-safe.
 */
public class HBaseStore<K, T extends Persistent> extends DataStoreBase<K, T> 
implements Configurable {

  public static final String PARSE_MAPPING_FILE_KEY = "gora.hbase.mapping.file";

  public static final String DEFAULT_FILE_NAME = "hbase-mapping.xml";

  private static final DocumentBuilder docBuilder;

  // a map from field name to hbase column
  private Map<String, HbaseColumn> columnMap;

  private List<HColumnDescriptor> colDescs;

  private String tableName;

  private HTable table;

  private Schema schema;

  private Configuration conf;
  
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

  public HBaseStore()  {
  }

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) {
    super.initialize(keyClass, persistentClass, properties);
    this.conf = new HBaseConfiguration();
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
  public T get(K key, String[] fields) throws IOException {
    Get get = new Get(toBytes(key));
    addFields(get, fields);
    Result result = table.get(get);
    return newInstance(result, fields);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void put(K key, T persistent) throws IOException {
    Schema schema = persistent.getSchema();
    StateManager stateManager = persistent.getStateManager();
    byte[] keyRaw = toBytes(key);
    Put put = new Put(keyRaw);
    Delete delete = new Delete(keyRaw);
    boolean hasPuts = false;
    boolean hasDeletes = false;
    Iterator<Entry<String, Schema>> iter =
      schema.getFieldSchemas().iterator();
    for (int i = 0; iter.hasNext(); i++) {
      Entry<String, Schema> field = iter.next();
      if (!stateManager.isDirty(persistent, i)) {
        continue;
      }
      Type type = field.getValue().getType();
      Object o = persistent.get(i);
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

  public void delete(T obj) {
    throw new RuntimeException("Not implemented yet");
  }
  
  @Override
  public void delete(K key) throws IOException {
    table.delete(new Delete(toBytes(key)));
  }

  @Override
  public void flush() throws IOException {
    table.flushCommits();
  }

  @Override
  public Query<K, T> newQuery() {
    return new HBaseQuery<K, T>(this);
  }
  
  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
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
    List<PartitionQuery<K,T>> partitions = new ArrayList<PartitionQuery<K,T>>(keys.getFirst().length); 
    for (int i = 0; i < keys.getFirst().length; i++) {
      String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
      getServerAddress().getHostname();
      byte[] startRow = query.getStartKey() != null ? toBytes(query.getStartKey()) : HConstants.EMPTY_START_ROW;
      byte[] stopRow = query.getEndKey() != null ? toBytes(query.getEndKey()) : HConstants.EMPTY_END_ROW;
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
                
                K startKey = HBaseByteInterface.fromBytes(keyClass, splitStart);
                K endKey = HBaseByteInterface.fromBytes(keyClass, splitStop);
                
                PartitionQuery<K, T> partition = new PartitionQueryImpl<K, T>(
                    query, startKey, endKey, regionLocation);
                partitions.add(partition);
      }
    }
    return partitions;
  }

  @Override
  public org.gora.query.Result<K, T> execute(Query<K, T> query)
      throws IOException {

    HBaseQuery<K, T> hQuery = (HBaseQuery<K, T>) query;
    ResultScanner scanner = createScanner(hQuery);
    
    org.gora.query.Result<K,T> result 
      = new HBaseScannerResult<K,T>(this,hQuery, scanner);
    
    return result; 
  }
  
  public ResultScanner createScanner(HBaseQuery<K, T> query) 
  throws IOException {
    final Scan scan = new Scan();
    if (query.getStartKey() != null) {
      scan.setStartRow(toBytes(query.getStartKey()));
    }
    if (query.getEndKey() != null) {
      scan.setStopRow(toBytes(query.getEndKey()));
    }
    addFields(scan, query);
    
    return table.getScanner(scan);
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

  private void addFields(Scan scan, HBaseQuery<K,T> query)
  throws IOException {
    String[] fields = query.getFields();
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
  public T newInstance(Result result, String[] fields)
  throws IOException {
    T persistent = newInstance();
    StateManager stateManager = persistent.getStateManager();
    Schema schema = persistent.getSchema();
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
        setField(persistent, field, map);
      } else {
        byte[] val =
          result.getValue(col.getFamily(), col.getQualifier());
        if (val == null) {
          continue;
        }
        setField(persistent, field, val);
      }
    }
    stateManager.clearDirty(persistent);
    return persistent;
  }



  @SuppressWarnings("unchecked")
  private void setField(T row, Field field, Map map) {
    row.set(field.pos(), new StatefulHashMap(map));
  }

  private void setField(T row, Field field, byte[] val) {
    row.set(field.pos(), fromBytes(field.schema(), val));
  }

  @SuppressWarnings("unchecked")
  private void parseMapping(String fileName)
  throws ClassNotFoundException, InstantiationException, IllegalAccessException,
  SecurityException, NoSuchFieldException {
    try {      
      InputStream stream =
        HBaseStore.class.getClassLoader().getResourceAsStream(fileName);
      Document doc = docBuilder.parse(stream);
      NodeWalker walker = new NodeWalker(doc.getFirstChild());
      boolean processInfo = false;

      while (walker.hasNext()) {
        Node node = walker.nextNode();
        if (node.getNodeType() == Node.TEXT_NODE) {
          continue;
        }
        if (node.getNodeName().equals("table")) {
          processInfo = true;
          Class<K> currentKeyClass =
            (Class<K>) Class.forName(XmlUtils.getAttribute(node, "keyClass"));
          Class<T> currentPersistentClass =
            (Class<T>) Class.forName(XmlUtils.getAttribute(node, "persistentClass"));
          System.out.println(currentKeyClass);
          System.out.println(currentPersistentClass);
          System.out.println(getKeyClass());
          System.out.println(getPersistentClass());
          if (!currentKeyClass.equals(getKeyClass()) || !currentPersistentClass.equals(getPersistentClass())) {
            processInfo = false;
            continue;
          }

          tableName = XmlUtils.getAttribute(node, "name");
          System.out.println(tableName);
          HBaseAdmin admin = new HBaseAdmin(new HBaseConfiguration(getConf()));
          if (admin.tableExists(tableName)) {
            table = new HTable(tableName);
            table.setAutoFlush(false);
          } else {
            table = null;
          }
          schema = getPersistentClass().newInstance().getSchema();
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
    flush();
    if(table != null) 
      table.close();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
