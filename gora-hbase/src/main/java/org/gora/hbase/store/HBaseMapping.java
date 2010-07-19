
package org.gora.hbase.store;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mapping definitions for HBase
 */
public class HBaseMapping {

  private Map<String, HTableDescriptor> tableDescriptors 
    = new HashMap<String, HTableDescriptor>();
  
  //name of the primary table
  private String tableName; 
  
  // a map from field name to hbase column
  private Map<String, HBaseColumn> columnMap = 
    new HashMap<String, HBaseColumn>();
  
  public HBaseMapping() {
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public void addTable(String tableName) {
    if(!tableDescriptors.containsKey(tableName)) {
      tableDescriptors.put(tableName, new HTableDescriptor(tableName));
    }
  }
  
  public HTableDescriptor getTable() {
    return getTable(tableName);
  }
  
  public HTableDescriptor getTable(String tableName) {
    return tableDescriptors.get(tableName);
  }
  
  public void addColumnFamily(String tableName, String familyName
      , String compression, String blockCache, String blockSize, String bloomFilter
      , String maxVersions, String timeToLive, String inMemory, String mapFileIndexInterval) {
    
    HColumnDescriptor columnDescriptor = addColumnFamily(tableName, familyName);
    
    if(compression != null)
      columnDescriptor.setCompressionType(Algorithm.valueOf(compression));
    if(blockCache != null)
      columnDescriptor.setBlockCacheEnabled(Boolean.parseBoolean(blockCache));
    if(blockSize != null)
      columnDescriptor.setBlocksize(Integer.parseInt(blockSize));
    if(bloomFilter != null)
      columnDescriptor.setBloomfilter(Boolean.parseBoolean(bloomFilter));
    if(maxVersions != null)
      columnDescriptor.setMaxVersions(Integer.parseInt(maxVersions));
    if(timeToLive != null)
      columnDescriptor.setTimeToLive(Integer.parseInt(timeToLive));
    if(inMemory != null)
      columnDescriptor.setInMemory(Boolean.parseBoolean(inMemory));
    if(mapFileIndexInterval != null)
      columnDescriptor.setMapFileIndexInterval(Integer.parseInt(mapFileIndexInterval));
    
    getTable(tableName).addFamily(columnDescriptor);
  }
  
  public HColumnDescriptor addColumnFamily(String tableName, String familyName) {
    HTableDescriptor tableDescriptor = getTable(tableName);
    HColumnDescriptor columnDescriptor =  tableDescriptor.getFamily(Bytes.toBytes(familyName));
    if(columnDescriptor == null) {
      columnDescriptor = new HColumnDescriptor(familyName);
      tableDescriptor.addFamily(columnDescriptor);
    }
    return columnDescriptor;
  }
  
  public void addField(String fieldName, String tableName, String family, String qualifier) {
    byte[] familyBytes = Bytes.toBytes(family);
    byte[] qualifierBytes = qualifier == null ? null : Bytes.toBytes(qualifier);
    
    HBaseColumn column = new HBaseColumn(tableName, familyBytes, qualifierBytes);
    columnMap.put(fieldName, column);
  }
 
  public HBaseColumn getColumn(String fieldName) {
    return columnMap.get(fieldName);
  }
}