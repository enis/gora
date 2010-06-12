
package org.gora.sql.store;

import java.util.HashMap;

class SqlMapping {

  private String tableName;
  private HashMap<String, Column> fields;
  private String primaryKeyField;
  
  public SqlMapping() {
    fields = new HashMap<String, Column>();
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public void addField(String fieldname, String column) {
    fields.put(fieldname, new Column(column));
  }
  
  public void addField(String fieldname, String columnName, String jdbcType, boolean isPrimaryKey
      , int length, int scale) {
    Column column = new Column(columnName, jdbcType, isPrimaryKey, length, scale);
    fields.put(fieldname, column);
    if(isPrimaryKey)
      setPrimaryKeyField(fieldname);
  }
  
  public Column getColumn(String fieldname) {
    return fields.get(fieldname);
  }
  
  public String getPrimaryKeyField() {
    return primaryKeyField;
  }
  
  public void setPrimaryKeyField(String primaryKeyField) {
    this.primaryKeyField = primaryKeyField;
  }
  
  public Column getPrimaryColumn() {
    return getColumn(primaryKeyField);
  }
  
  public HashMap<String, Column> getFields() {
    return fields;
  }
}
