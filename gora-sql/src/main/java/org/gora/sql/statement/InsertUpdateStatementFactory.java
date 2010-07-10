package org.gora.sql.statement;

import org.gora.persistency.Persistent;
import org.gora.sql.store.SqlMapping;
import org.gora.sql.store.SqlStore;

public class InsertUpdateStatementFactory {

  public static <K, T extends Persistent>
  InsertUpdateStatement<K, T> createStatement(SqlStore<K, T> store,
      SqlMapping mapping, String dbProductName) {
    if (dbProductName.equalsIgnoreCase("mysql")) {
      return new MySqlInsertUpdateStatement<K, T>(store, mapping, mapping.getTableName());
    } else if (dbProductName.equalsIgnoreCase("HSQL Database Engine")) {
      return new HSqlInsertUpdateStatement<K, T>(store, mapping, mapping.getTableName());
    }
    throw new RuntimeException("Database " + dbProductName + " is not supported yet.");
  }
}
