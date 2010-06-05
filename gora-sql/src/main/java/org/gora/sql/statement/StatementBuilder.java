package org.gora.sql.statement;


/**
 * A class for building simple sql statements. This class captures only
 * a subset that is needed for our use-cases. 
 */
public class StatementBuilder {

  /*---------------- SELECT  ----------------*/
  
  public static String select(String selectList, String from
      , String where, String orderBy) {
    return new SelectStatement(selectList, from, where, orderBy).toString();
  }
  
  public static String select(String selectList, String from
      , String where) {
    return select(selectList, from, where, null);
  }
  
  public static String select(String selectList, String from) {
    return select(selectList, from, null, null);
  }
  
  public static String select(String from) {
    return select("*", from, null, null);
  }
  
  
  /*---------------- INSERT  ----------------*/
  
  public static String insert(String tableName, String[] columnNames, 
      Object[] values) {
    return new InsertStatement(tableName, columnNames).toString();
  }
 
  public static String insert(String tableName, String[] values) {
    return insert(tableName, null, values);
  }

  /**
   * Returns an INSERT statement, but instead of filling the values with the 
   * given values array, returns a statement with ?,?,.. as values. 
   * <p>
   * For example the following statement 
   * <pre>
   * SqlBuilder.insertPrepare("foo", 3)
   * </pre>
   * would return :
   * <pre>
   * INSERT INTO foo VALUES (?, ?, ?);
   * </pre>
   * @param tableName the table to insert to
   * @param numValues the number of '?' as values
   */
  public static String insertPrepare(String tableName, int numValues) {   
    return insertPrepare(tableName, null, numValues);
  }

  /**
   * Returns an INSERT statement, but instead of filling the values with the 
   * given values array, returns a statement with ?,?,.. as values. 
   * <p>
   * For example the following statement 
   * <pre>
   * SqlBuilder.insertPrepare("foo", new String{"col1","col2"} )
   * </pre>
   * would return :
   * <pre>
   * INSERT INTO foo(col1, col2) VALUES (?, ?);
   * </pre>
   * @param tableName the table to insert to
   * @param numValues the number of '?' as values
   */
  public static String insertPrepare(String tableName, String[] columns) {   
    return insertPrepare(tableName, columns, columns.length);
  }
  
  private static String insertPrepare(String tableName, String[] columns, int numValues) {
    StringBuilder builder = new StringBuilder("?");
    for(int i=1; i<numValues; i++) {
      builder.append(",?");
    }
    return insert(tableName, columns, arr(builder.toString()));
  }
  
  private static String[] arr(String... values) {
    return values;
  }
  
}
