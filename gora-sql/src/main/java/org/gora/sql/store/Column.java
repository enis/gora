
package org.gora.sql.store;

class Column {

  private String name;
  private String jdbcType;
  private boolean isPrimaryKey;
  private int length = -1;
  private int scale = -1;
  
  public Column() {
  }
  
  public Column(String name) {
    this.name = name;
  }
  
  public Column(String name, String jdbcType, boolean isPrimaryKey) {
    this.name = name;
    this.jdbcType = jdbcType;
    this.isPrimaryKey = isPrimaryKey;
  }
  
  public Column(String name, boolean isPrimaryKey) {
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public String getJdbcType() {
    return jdbcType;
  }
  
  public void setJdbcType(String jdbcType) {
    this.jdbcType = jdbcType;
  }
  
  public void setPrimaryKey() {
    this.isPrimaryKey = true;
  }
  
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }
  
  public void setLength(int length) {
    this.length = length;
  }
  
  public int getLength() {
    return length;
  }
  
  public int getScale() {
    return scale;
  }
  
  public void setScale(int scale) {
    this.scale = scale;
  }
  
  public int getScaleOrLength() {
    return length > 0 ? length : scale;  
  }
}
