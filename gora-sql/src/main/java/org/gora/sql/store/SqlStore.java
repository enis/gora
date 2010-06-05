
package org.gora.sql.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.util.Utf8;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.sql.query.SqlQuery;
import org.gora.sql.query.SqlResult;
import org.gora.sql.statement.InsertStatement;
import org.gora.sql.statement.SelectStatement;
import org.gora.sql.statement.WhereClause;
import org.gora.store.DataStoreFactory;
import org.gora.store.impl.DataStoreBase;
import org.gora.util.AvroUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class SqlStore<K, T extends Persistent> extends DataStoreBase<K, T> {

  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = "jdbc.driver";

  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "jdbc.url";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "jdbc.user";

  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = 
    "mapreduce.jdbc.input.table.name";

  public static final String DEFAULT_MAPPING_FILE = "gora-sql-mapping.xml";  

  private String jdbcDriverClass;
  private String jdbcUrl;
  private String jdbcUsername;
  private String jdbcPassword;

  private SqlMapping mapping;

  private Connection connection; //no connection pooling yet

  private HashSet<PreparedStatement> writeCache;

  private int keySqlType;

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass,
      Properties properties) throws IOException {
    super.initialize(keyClass, persistentClass, properties);

    jdbcDriverClass = DataStoreFactory.findProperty(properties
        , this, DRIVER_CLASS_PROPERTY, null);
    jdbcUrl = DataStoreFactory.findProperty(properties
        , this, URL_PROPERTY, null);
    jdbcUsername = DataStoreFactory.findProperty(properties
        , this, USERNAME_PROPERTY, null);
    jdbcPassword = DataStoreFactory.findProperty(properties
        , this, PASSWORD_PROPERTY, null);

    String mappingFile = DataStoreFactory.getMappingFile(properties, this
        , DEFAULT_MAPPING_FILE);
    mapping = readMapping(mappingFile);

    connection = getConnection();    

    writeCache = new HashSet<PreparedStatement>();

    keySqlType = SqlTypeInterface.getSqlType(keyClass);
  }

  @Override
  public void close() throws IOException {
    if(connection!=null) {
      try {
        connection.commit();
        connection.close();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }
  }

  @Override
  public void createSchema() throws IOException {
  }

  @Override
  public boolean delete(K key) throws IOException {
    return false;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) throws IOException {
    return 0;
  }

  @Override
  public void deleteSchema() throws IOException {
  }

  @Override
  public void flush() throws IOException {
    synchronized (writeCache) {
      for(PreparedStatement stmt : writeCache) {
        try {
          stmt.executeBatch();
        } catch (SQLException ex) {
          throw new IOException(ex);
        }
      }
      writeCache.clear();  
    }
  }

  @Override
  public T get(K key, String[] requestFields) throws IOException {
    requestFields = getFieldsToQuery(requestFields);

    ResultSet resultSet = null;
    try {
      WhereClause where = new WhereClause();
      SelectStatement select = new SelectStatement(mapping.getTableName());
      select.setWhere(where);

      for (int i = 0; i < requestFields.length; i++) {
        Column column = mapping.getColumn(requestFields[i]);

        select.addToSelectList(column.getName());
      }

      Column primaryColumn = mapping.getPrimaryColumn();

      where.addEqualsPart(primaryColumn.getName(), "?");
      PreparedStatement statement = getConnection().prepareStatement(select.toString());

      setObject(statement, 1, key, keySqlType, primaryColumn);

      resultSet = statement.executeQuery();
      
      if(!resultSet.next()) { //no matching result
        return null;
      }

      return readObject(resultSet, newPersistent(), requestFields);
    } catch (SQLException ex) {
      throw new IOException(ex);
    } finally {
      if(resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException ignore) { }
      }
    }
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) throws IOException {
    query.setFields(getFieldsToQuery(query.getFields()));
    String[] requestFields = query.getFields();

    ResultSet resultSet = null;
    try {
      WhereClause where = new WhereClause();
      SelectStatement select = new SelectStatement(mapping.getTableName());
      select.setWhere(where);

      for (int i = 0; i < requestFields.length; i++) {
        Column column = mapping.getColumn(requestFields[i]);

        select.addToSelectList(column.getName());
      }

      Column primaryColumn = mapping.getPrimaryColumn();

      if (query.getKey() != null) {
        where.addEqualsPart(primaryColumn.getName(), "?");  
      } else {
        if (query.getStartKey() != null) {
          where.addGreaterThanEqPart(primaryColumn.getName(), "?");
        }
        if(query.getEndKey() != null) {
          where.addLessThanEqPart(primaryColumn.getName(), "?");
        }
      }
      
      if(query.getLimit() > 0) {
        select.setLimit(query.getLimit());
      }
      
      PreparedStatement statement = getConnection().prepareStatement(select.toString());

      if(query.getKey() != null) {
        setObject(statement, 1, query.getKey(), keySqlType, primaryColumn);  
      }

      resultSet = statement.executeQuery();

      return new SqlResult<K, T>(this, query, resultSet);
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }
  
  public T readObject(ResultSet rs, T persistent
      , String[] requestFields) throws SQLException, IOException {
    if(rs == null) {
      return null;
    }

    Schema schema = persistent.getSchema();
    
    for(int i=0; i<requestFields.length; i++) {
      String f = requestFields[i];
      Field field = fieldMap.get(f);
      Type type = field.schema().getType();
      
      switch(type) {
        case MAP:
          break;
        case ARRAY:
          break;
        case BOOLEAN:
          persistent.put(field.pos(), rs.getBoolean(i+1));
          break;
        case BYTES:
          persistent.put(field.pos(), ByteBuffer.wrap(rs.getBytes(i+1)));
          break;
        case DOUBLE:
          persistent.put(field.pos(), rs.getDouble(i+1));
          break;
        case ENUM:
          Object val = AvroUtils.getEnumValue(schema, rs.getString(i+1));
          persistent.put(field.pos(), val);
          break;
        case FIXED:
          ((SpecificFixed)persistent.get(i)).bytes(rs.getBytes(i+1));
          break;
        case FLOAT:
          persistent.put(field.pos(), rs.getFloat(i+1));
          break;
        case INT:
          persistent.put(field.pos(), rs.getInt(i+1));
          break;
        case LONG:
          persistent.put(field.pos(), rs.getLong(i+1));
          break;
        case NULL:
          break;
        case RECORD:
          break;
        case STRING:
          persistent.put(field.pos(), new Utf8(rs.getString(i+1)));
          break;
        case UNION:
          throw new IOException("Union is not supported yet");
      }
    }
    return persistent;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
  throws IOException {
    return null;
  }

  @Override
  public Query<K, T> newQuery() {
    return new SqlQuery<K, T>(this);
  }

  @Override
  public void put(K key, T persistent) throws IOException {
    try {
      //TODO: INSERT or UPDATE 

      Schema schema = persistent.getSchema();
      StateManager stateManager = persistent.getStateManager();

      List<Field> fields = schema.getFields();

      InsertStatement insertStatement = new InsertStatement(mapping.getTableName());
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        if (!stateManager.isDirty(persistent, i)) {
          continue;
        }

        Column column = mapping.getColumn(field.name());
        insertStatement.addColumnName(column.getName());
      }

      //jdbc already should cache the ps
      PreparedStatement insert = connection.prepareStatement(insertStatement.toString());

      int psIndex = 1; //the index in the statement
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        if (!stateManager.isDirty(persistent, i)) {
          continue;
        }
        Column column = mapping.getColumn(field.name());
        Schema fieldSchema = field.schema();
        Object fieldValue = persistent.get(i);

        setObject(insert, psIndex, fieldValue, fieldSchema, column);

        psIndex++;
      }

      insert.addBatch();

      synchronized (writeCache) {
        writeCache.add(insert);  
      }

    }catch (Exception ex) {
      throw new IOException(ex);
    } 
  }

  /**
   * Sets the object to the preparedStatement by it's schema
   */
  protected <V> void setObject(PreparedStatement statement, int index, V object
      , Schema schema, Column column) throws SQLException, IOException {

    Type type = schema.getType();

    switch(type) {
      case MAP:
        break;
      case ARRAY:
        break;
      case BOOLEAN:
        statement.setBoolean(index, (Boolean)object); 
        break;
      case BYTES:
        setBytes(statement, index, ((ByteBuffer)object).array(), column);
        break;
      case DOUBLE:
        statement.setDouble(index, (Double)object);
        break;
      case ENUM:
        statement.setString(index, ((Enum)object).name());
        break;
      case FIXED:
        setBytes(statement, index, ((GenericFixed)object).bytes(), column);
        break;
      case FLOAT:
        statement.setFloat(index, (Float)object);
        break;
      case INT:
        statement.setInt(index, (Integer)object);
        break;
      case LONG:
        statement.setLong(index, (Long)object);
        break;
      case NULL:
        break;
      case RECORD:
        break;
      case STRING:
        statement.setString(index, ((Utf8)object).toString());
        break;
      case UNION:
        throw new IOException("Union is not supported yet");
    }
  }

  protected <V> void setObject(PreparedStatement statement, int index, V object
      , int objectType, Column column) throws SQLException, IOException {
    statement.setObject(index, object, objectType, column.getScaleOrLength());
  }

  protected void setBytes(PreparedStatement insert, int index, byte[] value, Column column) 
  throws SQLException   {
    if("BLOB".equalsIgnoreCase(column.getJdbcType())) {
      Blob blob = connection.createBlob();
      blob.setBytes(0, value);
      insert.setBlob(index, blob);
    } else {
      insert.setBytes(index, value);
    }
  }

  @Override
  public boolean schemaExists() throws IOException {
    return false;
  }


  protected Connection getConnection() throws IOException {
    try {

      Connection connection = null;

      Class.forName(jdbcDriverClass);
      if(jdbcUsername == null) {
        connection = DriverManager.getConnection(jdbcUrl);
      } else {
        connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, 
            jdbcPassword);
      }

      connection.setAutoCommit(false);

      return connection;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  protected SqlMapping readMapping(String filename) throws IOException {

    SqlMapping mapping = new SqlMapping();

    try {
      SAXBuilder builder = new SAXBuilder();
      Document doc = builder.build(getClass().getClassLoader()
          .getResourceAsStream(filename));

      List<Element> tables = doc.getRootElement().getChildren("table");

      for(Element table: tables) {
        if(table.getAttributeValue("keyClass").equals(keyClass.getCanonicalName())
            && table.getAttributeValue("persistentClass").equals(
                persistentClass.getCanonicalName())) {

          mapping.setTableName(table.getAttributeValue("name"));
          List<Element> fields = table.getChild("fields").getChildren("field");

          for(Element field:fields) {
            String name = field.getAttributeValue("name");
            String column = field.getAttributeValue("column");

            String primaryKeyStr = field.getAttributeValue("primarykey");
            boolean isPrimaryKey = false;
            if(primaryKeyStr != null)
              isPrimaryKey = Boolean.parseBoolean(primaryKeyStr);

            mapping.addField(name, column, isPrimaryKey);
          }

          break;
        }
      }

    } catch(Exception ex) {
      throw new IOException(ex);
    }

    return mapping;
  }

}
