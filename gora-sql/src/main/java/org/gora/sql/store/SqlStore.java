
package org.gora.sql.store;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gora.avro.PersistentDatumReader;
import org.gora.avro.PersistentDatumWriter;
import org.gora.persistency.Persistent;
import org.gora.persistency.StateManager;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.query.impl.PartitionQueryImpl;
import org.gora.sql.query.SqlQuery;
import org.gora.sql.query.SqlResult;
import org.gora.sql.statement.InsertStatement;
import org.gora.sql.statement.SelectStatement;
import org.gora.sql.statement.WhereClause;
import org.gora.sql.store.SqlTypeInterface.JdbcType;
import org.gora.store.DataStoreFactory;
import org.gora.store.impl.DataStoreBase;
import org.gora.util.AvroUtils;
import org.gora.util.IOUtils;
import org.gora.util.StringUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import com.healthmarketscience.sqlbuilder.CreateTableQuery;
import com.healthmarketscience.sqlbuilder.CreateTableQuery.ColumnConstraint;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

public class SqlStore<K, T extends Persistent> extends DataStoreBase<K, T> {

  private static final Log log = LogFactory.getLog(SqlStore.class);

  /** The JDBC Driver class name */
  protected static final String DRIVER_CLASS_PROPERTY = "jdbc.driver";

  /** JDBC Database access URL */
  protected static final String URL_PROPERTY = "jdbc.url";

  /** User name to access the database */
  protected static final String USERNAME_PROPERTY = "jdbc.user";

  /** Password to access the database */
  protected static final String PASSWORD_PROPERTY = "jdbc.password";

  protected static final String DEFAULT_MAPPING_FILE = "gora-sql-mapping.xml";

  private String jdbcDriverClass;
  private String jdbcUrl;
  private String jdbcUsername;
  private String jdbcPassword;

  private SqlMapping mapping;

  private Connection connection; //no connection pooling yet

  private HashSet<PreparedStatement> writeCache;

  private int keySqlType;

  private DbTable sqlTable;

  private PersistentDatumReader<T> datumReader;
  private PersistentDatumWriter<T> datumWriter;

  private BinaryDecoder decoder;

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

    sqlTable = createSqlTable(mapping);

    connection = getConnection();

    writeCache = new HashSet<PreparedStatement>();

    keySqlType = SqlTypeInterface.getSqlType(keyClass);

    datumReader = new PersistentDatumReader<T>(schema);
    datumWriter = new PersistentDatumWriter<T>(schema);



    if(autoCreateSchema) {
      createSchema();
    }

    //TODO: get the table metadata if the table exists previously, and fill sql types with correct information

    this.conf = getOrCreateConf();
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
    if(!schemaExists()) {

      log.info("creating schema: " + sqlTable.getAbsoluteName());

      CreateTableQuery query = new CreateTableQuery(sqlTable, true);

      for(Column column : mapping.getFields().values()) {
        ColumnConstraint constraint = getColumnConstraint(column);
        if(constraint != null)
        query.setColumnConstraint(sqlTable.findColumn(column.getName()), constraint);
      }

      try {
        PreparedStatement statement = connection.prepareStatement(query.validate().toString());

        statement.executeUpdate();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }
  }

  private ColumnConstraint getColumnConstraint(Column column) {
    if(column.isPrimaryKey()) {
      return ColumnConstraint.PRIMARY_KEY;
    }
    return null;
  }

  @Override
  public void deleteSchema() throws IOException {
    flush();
    if(schemaExists()) {
      try {
        log.info("dropping schema:" + sqlTable.getAbsoluteName());

        //DropQuery does not work
        PreparedStatement statement = connection.prepareStatement(
            "DROP TABLE " + sqlTable.getAbsoluteName());
        statement.executeUpdate();

        connection.commit();
      } catch (SQLException ex) {
        throw new IOException(ex);
      }
    }
  }

  @Override
  public boolean schemaExists() throws IOException {
    ResultSet resultSet = null;
    try {
      DatabaseMetaData metadata = connection.getMetaData();
      String tableName = mapping.getTableName();
      if(!metadata.storesMixedCaseIdentifiers()) {
        if(metadata.storesLowerCaseIdentifiers()) {
          tableName = tableName.toLowerCase();
        }
        else if(metadata.storesUpperCaseIdentifiers()) {
          tableName = tableName.toUpperCase();
        }
      }

      resultSet = metadata.getTables(null, null, tableName, null);

      if(resultSet.next())
        return true;

    } catch (Exception ex) {
      throw new IOException(ex);
    } finally {
      close(resultSet);
    }

    return false;
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
    try {
      connection.commit();
    } catch (SQLException ex) {
      throw new IOException(ex);
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
      Column primaryColumn = mapping.getPrimaryColumn();

//      boolean isPrimarySelected = false;
//      for (int i = 0; i < requestFields.length; i++) {
//        if(primaryColumn.getName().equals(primaryColumn)) {
//          isPrimarySelected = true;
//          break;
//        }
//      }
//      if(!isPrimarySelected) {
//        requestFields = StringUtils.append(requestFields, primaryColumn.getName());
//      }

      for (int i = 0; i < requestFields.length; i++) {
        Column column = mapping.getColumn(requestFields[i]);

        select.addToSelectList(column.getName());
      }


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
      close(resultSet);
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
      int offset = 1;

      if(query.getKey() != null) {
        setObject(statement, offset++, query.getKey(), keySqlType, primaryColumn);
      } else {
        if(query.getStartKey() != null) {
          setObject(statement, offset++, query.getStartKey(), keySqlType, primaryColumn);
        }
        if(query.getEndKey() != null) {
          setObject(statement, offset++, query.getEndKey(), keySqlType, primaryColumn);
        }
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

    for(int i=0; i<requestFields.length; i++) {
      String f = requestFields[i];
      Field field = fieldMap.get(f);
      Schema fieldSchema = field.schema();
      Type type = fieldSchema.getType();
      Column column = mapping.getColumn(field.name());

      switch(type) {
        case MAP:
          readField(rs, persistent.get(field.pos()), fieldSchema, column, i+1);
          break;
        case ARRAY:
          readField(rs, persistent.get(field.pos()), fieldSchema, column, i+1);
          break;
        case BOOLEAN:
          persistent.put(field.pos(), rs.getBoolean(i+1));
          break;
        case BYTES:
          persistent.put(field.pos(), ByteBuffer.wrap(getBytes(rs, fieldSchema, column, i+1)));
          break;
        case DOUBLE:
          persistent.put(field.pos(), rs.getDouble(i+1));
          break;
        case ENUM:
          Object val = AvroUtils.getEnumValue(fieldSchema, rs.getString(i+1));
          persistent.put(field.pos(), val);
          break;
        case FIXED:
          ((SpecificFixed)persistent.get(i)).bytes(getBytes(rs, fieldSchema, column, i+1));
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
          readField(rs, persistent.get(field.pos()), fieldSchema, column, i+1);
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

  protected byte[] getBytes(ResultSet resultSet, Schema schema, Column column, int index)
    throws SQLException, IOException {
    switch(column.getJdbcType()) {
      case BLOB          : Blob blob = resultSet.getBlob(index);
                           return IOUtils.readFully(blob.getBinaryStream());
      case BINARY        :
      case VARBINARY     : return resultSet.getBytes(index);
      case LONGVARBINARY : return IOUtils.readFully(resultSet.getBinaryStream(index));
    }
    return null;
  }

  protected Object readField(ResultSet resultSet, Object field
      , Schema schema, Column column, int index) throws SQLException, IOException {

    InputStream is = null;
    byte[] bytes = null;
    switch(column.getJdbcType()) {
      case BLOB          : Blob blob = resultSet.getBlob(index);
                           if (blob != null) is = blob.getBinaryStream(); break;
      case BINARY        :
      case VARBINARY     : bytes = resultSet.getBytes(index); break;
      case LONGVARBINARY : is = resultSet.getBinaryStream(index); break;
    }

    if(bytes!=null)
      return readField(field, schema, bytes);
    else if(is != null)
      return readField(field, schema, is);
    return field; //field is empty
  }

  protected Object readField(Object field, Schema schema, byte[] bytes)
    throws IOException {
    decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, decoder);
    return datumReader.read(field, schema, decoder);
  }

  protected Object readField(Object field, Schema schema, InputStream is)
    throws IOException {
    decoder = DecoderFactory.defaultFactory().createBinaryDecoder(is, decoder);
    return datumReader.read(field, schema, decoder);
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
  throws IOException {
    //TODO: implement this using Hadoop DB support

    ArrayList<PartitionQuery<K,T>> partitions = new ArrayList<PartitionQuery<K,T>>();
    partitions.add(new PartitionQueryImpl<K,T>(query));

    return partitions;
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
        setField(statement, column, schema, index, object);
        break;
      case ARRAY:
        setField(statement, column, schema, index, object);
        break;
      case BOOLEAN:
        statement.setBoolean(index, (Boolean)object);
        break;
      case BYTES:
        setBytes(statement, column, index, ((ByteBuffer)object).array());
        break;
      case DOUBLE:
        statement.setDouble(index, (Double)object);
        break;
      case ENUM:
        statement.setString(index, ((Enum)object).name());
        break;
      case FIXED:
        setBytes(statement, column, index, ((GenericFixed)object).bytes());
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
        setField(statement, column, schema, index, object);
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

  protected void setBytes(PreparedStatement statement, Column column, int index, byte[] value)
  throws SQLException   {

    switch(column.getJdbcType()) {
      case BLOB          : Blob blob = connection.createBlob();
                           blob.setBytes(1, value);
                           statement.setBlob(index, blob); break;
      case BINARY        :
      case VARBINARY     : statement.setBytes(index, value); break;
      case LONGVARBINARY : statement.setBinaryStream(index,
        new ByteArrayInputStream(value)); break;
    }
  }

  /** Serializes the field using Avro to a BLOB field */
  protected void setField(PreparedStatement statement, Column column, Schema schema
      , int index, Object object)
  throws IOException, SQLException {

    OutputStream os = null;
    Blob blob = null;
    switch(column.getJdbcType()) {
      case BLOB          : blob = connection.createBlob();
                           os = blob.setBinaryStream(1); break;
      case BINARY        :
      case VARBINARY     :
      case LONGVARBINARY : os = new ByteBufferOutputStream(); break;
    }

    IOUtils.serialize(os, datumWriter, schema, object);
    os.close();

    switch(column.getJdbcType()) {
      case BLOB          : statement.setBlob(index, blob); break;
      case BINARY        :
      case VARBINARY     : statement.setBytes(index
          , IOUtils.getAsBytes(((ByteBufferOutputStream)os).getBufferList())); break;
      case LONGVARBINARY : statement.setBinaryStream(index,
          new ByteBufferInputStream(((ByteBufferOutputStream)os).getBufferList())); break;
    }
  }

  protected Connection getConnection() throws IOException {
    try {
      Connection connection = null;

      Class.forName(jdbcDriverClass);
      if(jdbcUsername == null || jdbcUsername.length() == 0) {
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

  protected DbTable createSqlTable(SqlMapping mapping) {
    // create default schema
    DbSpec spec = new DbSpec();
    DbSchema schema = spec.addDefaultSchema();

    DbTable table = schema.addTable(mapping.getTableName());

    for(Map.Entry<String, Column> entry : mapping.getFields().entrySet()) {
      Column column = entry.getValue();
      Integer length =  column.getScaleOrLength();
      length = length > 0 ? length : null;
      table.addColumn(column.getName(), column.getJdbcType().getSqlType(), length);
    }

    return table;
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
            String fieldName = field.getAttributeValue("name");
            String columnName = field.getAttributeValue("column");

            String jdbcTypeStr = field.getAttributeValue("jdbc-type");

            String primaryKeyStr = field.getAttributeValue("primarykey");
            boolean isPrimaryKey = false;
            if(primaryKeyStr != null)
              isPrimaryKey = Boolean.parseBoolean(primaryKeyStr);

            int length = StringUtils.parseInt(field.getAttributeValue("length"), -1);
            int scale = StringUtils.parseInt(field.getAttributeValue("scale"), -1);

            JdbcType jdbcType;
            if(jdbcTypeStr == null) {
              Schema fieldSchema = schema.getField(fieldName).schema();
              jdbcType = SqlTypeInterface.getJdbcType(fieldSchema);
            } else {
              jdbcType = SqlTypeInterface.stringToJdbcType(jdbcTypeStr);
            }

            mapping.addField(fieldName, columnName, jdbcType, isPrimaryKey, length, scale);
          }

          break;
        }
      }

    } catch(Exception ex) {
      throw new IOException(ex);
    }

    return mapping;
  }

  private void close(ResultSet rs) {
    if(rs != null) {
      try {
        rs.close();
      } catch (SQLException ignore) { }
    }
  }

}
