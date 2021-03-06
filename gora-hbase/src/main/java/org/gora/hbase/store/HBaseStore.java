package org.gora.hbase.store;

import static org.gora.hbase.util.HBaseByteInterface.fromBytes;
import static org.gora.hbase.util.HBaseByteInterface.toBytes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.gora.hbase.query.HBaseGetResult;
import org.gora.hbase.query.HBaseQuery;
import org.gora.hbase.query.HBaseScannerResult;
import org.gora.hbase.util.HBaseByteInterface;
import org.gora.persistency.ListGenericArray;
import org.gora.persistency.Persistent;
import org.gora.persistency.State;
import org.gora.persistency.StateManager;
import org.gora.persistency.StatefulHashMap;
import org.gora.persistency.StatefulMap;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.impl.PartitionQueryImpl;
import org.gora.store.impl.DataStoreBase;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

/**
 * DataStore for HBase.
 *
 * <p> Note: HBaseStore is not yet thread-safe.
 */
public class HBaseStore<K, T extends Persistent> extends DataStoreBase<K, T>
implements Configurable {

  public static final Log log = LogFactory.getLog(HBaseStore.class);

  public static final String PARSE_MAPPING_FILE_KEY = "gora.hbase.mapping.file";

  @Deprecated
  private static final String DEPRECATED_MAPPING_FILE = "hbase-mapping.xml";
  public static final String DEFAULT_MAPPING_FILE = "gora-hbase-mapping.xml";

  private HBaseAdmin admin;

  private HTable table;

  private Configuration conf;

  private boolean autoCreateSchema = true;

  private HBaseMapping mapping;

  public HBaseStore()  {
  }

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException {
    super.initialize(keyClass, persistentClass, properties);
    this.conf = new HBaseConfiguration();

    admin = new HBaseAdmin(new HBaseConfiguration(getConf()));

    try {
      mapping = readMapping(getConf().get(PARSE_MAPPING_FILE_KEY, DEFAULT_MAPPING_FILE));
    } catch (FileNotFoundException ex) {
      try {
        mapping = readMapping(getConf().get(PARSE_MAPPING_FILE_KEY, DEPRECATED_MAPPING_FILE));
        log.warn(DEPRECATED_MAPPING_FILE + " is deprecated, please rename the file to "
            + DEFAULT_MAPPING_FILE);
      } catch (FileNotFoundException ex1) {
        throw ex; //throw the original exception
      } catch (Exception ex1) {
        log.warn(DEPRECATED_MAPPING_FILE + " is deprecated, please rename the file to "
            + DEFAULT_MAPPING_FILE);
        throw new RuntimeException(ex1);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if(autoCreateSchema) {
      createSchema();
    }

    table = new HTable(mapping.getTableName());
  }

  @Override
  public String getSchemaName() {
    return mapping.getTableName();
  }

  @Override
  public void createSchema() throws IOException {
    if(admin.tableExists(mapping.getTableName())) {
      return;
    }
    HTableDescriptor tableDesc = mapping.getTable();

    admin.createTable(tableDesc);
  }

  @Override
  public void deleteSchema() throws IOException {
    if(!admin.tableExists(mapping.getTableName())) {
      if(table != null) {
        table.getWriteBuffer().clear();
      }
      return;
    }
    admin.disableTable(mapping.getTableName());
    admin.deleteTable(mapping.getTableName());
  }

  @Override
  public boolean schemaExists() throws IOException {
    return admin.tableExists(mapping.getTableName());
  }

  @Override
  public T get(K key, String[] fields) throws IOException {
    fields = getFieldsToQuery(fields);
    Get get = new Get(toBytes(key));
    addFields(get, fields);
    Result result = table.get(get);
    return newInstance(result, fields);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void put(K key, T persistent) throws IOException {
    Schema schema = persistent.getSchema();
    StateManager stateManager = persistent.getStateManager();
    byte[] keyRaw = toBytes(key);
    Put put = new Put(keyRaw);
    Delete delete = new Delete(keyRaw);
    boolean hasPuts = false;
    boolean hasDeletes = false;
    Iterator<Field> iter = schema.getFields().iterator();
    for (int i = 0; iter.hasNext(); i++) {
      Field field = iter.next();
      if (!stateManager.isDirty(persistent, i)) {
        continue;
      }
      Type type = field.schema().getType();
      Object o = persistent.get(i);
      HBaseColumn hcol = mapping.getColumn(field.name());
      switch(type) {
        case MAP:
          if(o instanceof StatefulMap) {
            StatefulHashMap<Utf8, ?> map = (StatefulHashMap<Utf8, ?>) o;
            for (Entry<Utf8, State> e : map.states().entrySet()) {
              Utf8 mapKey = e.getKey();
              switch (e.getValue()) {
                case DIRTY:
                  byte[] qual = Bytes.toBytes(mapKey.toString());
                  byte[] val = toBytes(map.get(mapKey), field.schema().getValueType());
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
            Set<Map.Entry> set = ((Map)o).entrySet();
            for(Entry entry: set) {
              byte[] qual = toBytes(entry.getKey());
              byte[] val = toBytes(entry.getValue());
              put.add(hcol.getFamily(), qual, val);
              hasPuts = true;
            }
          }
          break;
        case ARRAY:
          if(o instanceof GenericArray) {
            GenericArray arr = (GenericArray) o;
            int j=0;
            for(Object item : arr) {
              byte[] val = toBytes(item);
              put.add(hcol.getFamily(), Bytes.toBytes(j++), val);
              hasPuts = true;
            }
          }
          break;
        default:
          put.add(hcol.getFamily(), hcol.getQualifier(), toBytes(o, field.schema()));
          hasPuts = true;
          break;
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

  /**
   * Deletes the object with the given key.
   * @return always true
   */
  @Override
  public boolean delete(K key) throws IOException {
    table.delete(new Delete(toBytes(key)));
    //HBase does not return success information and executing a get for
    //success is a bit costly
    return true;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) throws IOException {

    String[] fields = getFieldsToQuery(query.getFields());
    //find whether all fields are queried, which means that complete
    //rows will be deleted
    boolean isAllFields = Arrays.equals(fields
        , getBeanFactory().getCachedPersistent().getFields());

    org.gora.query.Result<K, T> result = query.execute();

    ArrayList<Delete> deletes = new ArrayList<Delete>();
    while(result.next()) {
      Delete delete = new Delete(toBytes(result.getKey()));
      deletes.add(delete);
      if(!isAllFields) {
        addFields(delete, query);
      }
    }
    //TODO: delete by timestamp, etc

    table.delete(deletes);

    return deletes.size();
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
      byte[] startRow = query.getStartKey() != null ? toBytes(query.getStartKey())
          : HConstants.EMPTY_START_ROW;
      byte[] stopRow = query.getEndKey() != null ? toBytes(query.getEndKey())
          : HConstants.EMPTY_END_ROW;

      // determine if the given start an stop key fall into the region
      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
          Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
          (stopRow.length == 0 ||
              Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {

        byte[] splitStart = (startRow.length == 0 ||
          Bytes.compareTo(keys.getFirst()[i], startRow) >= 0) ?
            keys.getFirst()[i] : startRow;

        byte[] splitStop = (stopRow.length == 0 ||
            Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) ?
            keys.getSecond()[i] : stopRow;

        K startKey = Arrays.equals(HConstants.EMPTY_START_ROW, splitStart) ?
            null : HBaseByteInterface.fromBytes(keyClass, splitStart);
        K endKey = Arrays.equals(HConstants.EMPTY_END_ROW, splitStop) ?
            null : HBaseByteInterface.fromBytes(keyClass, splitStop);

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

    //check if query.fields is null
    query.setFields(getFieldsToQuery(query.getFields()));

    if(query.getStartKey() != null && query.getStartKey().equals(
        query.getEndKey())) {
      Get get = new Get(toBytes(query.getStartKey()));
      addFields(get, query.getFields());
      addTimeRange(get, query);
      Result result = table.get(get);
      return new HBaseGetResult<K,T>(this, query, result);
    } else {
      ResultScanner scanner = createScanner(query);

      org.gora.query.Result<K,T> result
      = new HBaseScannerResult<K,T>(this,query, scanner);

      return result;
    }
  }

  public ResultScanner createScanner(Query<K, T> query)
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

  private void addFields(Get get, String[] fieldNames) {
    for (String f : fieldNames) {
      HBaseColumn col = mapping.getColumn(f);
      Schema fieldSchema = fieldMap.get(f).schema();

      switch (fieldSchema.getType()) {
        case MAP:
        case ARRAY:
          get.addFamily(col.family); break;
        default:
          get.addColumn(col.family, col.qualifier); break;
      }
    }
  }

  private void addFields(Scan scan, Query<K,T> query)
  throws IOException {
    String[] fields = query.getFields();
    for (String f : fields) {
      HBaseColumn col = mapping.getColumn(f);
      Schema fieldSchema = fieldMap.get(f).schema();
      switch (fieldSchema.getType()) {
        case MAP:
        case ARRAY:
          scan.addFamily(col.family); break;
        default:
          scan.addColumn(col.family, col.qualifier); break;
      }
    }
  }

  //TODO: HBase Get, Scan, Delete should extend some common interface with addFamily, etc
  private void addFields(Delete delete, Query<K,T> query)
    throws IOException {
    String[] fields = query.getFields();
    for (String f : fields) {
      HBaseColumn col = mapping.getColumn(f);
      Schema fieldSchema = fieldMap.get(f).schema();
      switch (fieldSchema.getType()) {
        case MAP:
        case ARRAY:
          delete.deleteFamily(col.family); break;
        default:
          delete.deleteColumn(col.family, col.qualifier); break;
      }
    }
  }

  private void addTimeRange(Get get, Query<K, T> query) throws IOException {
    if(query.getStartTime() > 0 || query.getEndTime() > 0) {
      if(query.getStartTime() == query.getEndTime()) {
        get.setTimeStamp(query.getStartTime());
      } else {
        long startTime = query.getStartTime() > 0 ? query.getStartTime() : 0;
        long endTime = query.getEndTime() > 0 ? query.getEndTime() : Long.MAX_VALUE;
        get.setTimeRange(startTime, endTime);
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public T newInstance(Result result, String[] fields)
  throws IOException {
    if(result == null || result.isEmpty())
      return null;

    T persistent = newPersistent();
    StateManager stateManager = persistent.getStateManager();
    for (String f : fields) {
      HBaseColumn col = mapping.getColumn(f);
      Field field = fieldMap.get(f);
      Schema fieldSchema = field.schema();
      switch(fieldSchema.getType()) {
        case MAP:
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
          break;
        case ARRAY:
          qualMap = result.getFamilyMap(col.getFamily());
          if (qualMap == null) {
            continue;
          }
          valueSchema = fieldSchema.getElementType();
          ArrayList arrayList = new ArrayList();
          for (Entry<byte[], byte[]> e : qualMap.entrySet()) {
            arrayList.add(fromBytes(valueSchema, e.getValue()));
          }
          ListGenericArray arr = new ListGenericArray(fieldSchema, arrayList);
          setField(persistent, field, arr);
          break;
        default:
          byte[] val =
            result.getValue(col.getFamily(), col.getQualifier());
          if (val == null) {
            continue;
          }
          setField(persistent, field, val);
          break;
      }
    }
    stateManager.clearDirty(persistent);
    return persistent;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void setField(T persistent, Field field, Map map) {
    persistent.put(field.pos(), new StatefulHashMap(map));
  }

  private void setField(T persistent, Field field, byte[] val)
  throws IOException {
    persistent.put(field.pos(), fromBytes(field.schema(), val));
  }

  @SuppressWarnings("rawtypes")
  private void setField(T persistent, Field field, GenericArray list) {
    persistent.put(field.pos(), list);
  }

  @SuppressWarnings("unchecked")
  private HBaseMapping readMapping(String filename) throws IOException {

    HBaseMapping mapping = new HBaseMapping();

    try {
      SAXBuilder builder = new SAXBuilder();
      Document doc = builder.build(getClass().getClassLoader()
          .getResourceAsStream(filename));
      Element root = doc.getRootElement();

      List<Element> tableElements = root.getChildren("table");
      for(Element tableElement : tableElements) {
        String tableName = tableElement.getAttributeValue("name");
        mapping.addTable(tableName);

        List<Element> fieldElements = tableElement.getChildren("field");
        for(Element fieldElement : fieldElements) {
          String familyName  = fieldElement.getAttributeValue("name");
          String compression = fieldElement.getAttributeValue("compression");
          String blockCache  = fieldElement.getAttributeValue("blockCache");
          String blockSize   = fieldElement.getAttributeValue("blockSize");
          String bloomFilter = fieldElement.getAttributeValue("bloomFilter");
          String maxVersions = fieldElement.getAttributeValue("maxVersions");
          String timeToLive  = fieldElement.getAttributeValue("timeToLive");
          String inMemory    = fieldElement.getAttributeValue("inMemory");
          String mapFileIndexInterval  = tableElement.getAttributeValue("mapFileIndexInterval");

          mapping.addColumnFamily(tableName, familyName, compression, blockCache, blockSize
              , bloomFilter, maxVersions, timeToLive, inMemory, mapFileIndexInterval);
        }
      }

      List<Element> classElements = root.getChildren("class");
      for(Element classElement: classElements) {
        if(classElement.getAttributeValue("keyClass").equals(keyClass.getCanonicalName())
            && classElement.getAttributeValue("name").equals(
                persistentClass.getCanonicalName())) {

          String tableName = getSchemaName(classElement.getAttributeValue("table"), persistentClass);
          mapping.addTable(tableName);
          mapping.setTableName(tableName);

          List<Element> fields = classElement.getChildren("field");
          for(Element field:fields) {
            String fieldName =  field.getAttributeValue("name");
            String family =  field.getAttributeValue("family");
            String qualifier = field.getAttributeValue("qualifier");
            mapping.addField(fieldName, mapping.getTableName(), family, qualifier);
            mapping.addColumnFamily(mapping.getTableName(), family);//implicit family definition
          }

          break;
        }
      }
    } catch(IOException ex) {
      throw ex;
    } catch(Exception ex) {
      throw new IOException(ex);
    }

    return mapping;
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
