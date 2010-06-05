
package org.gora.sql.query;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.query.impl.ResultBase;
import org.gora.sql.store.SqlStore;
import org.gora.store.DataStore;

public class SqlResult<K, T extends Persistent> extends ResultBase<K, T> {

  private ResultSet resultSet;
  
  public SqlResult(DataStore<K, T> dataStore, Query<K, T> query, ResultSet resultSet) {
    super(dataStore, query);
    this.resultSet = resultSet;
  }

  @Override
  protected boolean nextInner() throws IOException {
    try {
      if(!resultSet.next()) { //no matching result
        return false;
      }
      
      persistent = ((SqlStore<K,T>)dataStore).readObject(
          resultSet, persistent, query.getFields());
      
      return true;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      resultSet.close();
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
  
}
