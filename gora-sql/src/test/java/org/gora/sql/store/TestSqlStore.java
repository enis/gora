
package org.gora.sql.store;

import java.io.IOException;

import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.gora.sql.GoraSqlTestDriver;
import org.gora.store.DataStore;
import org.gora.store.DataStoreTestBase;

/**
 * Test case for {@link SqlStore}
 */
public class TestSqlStore extends DataStoreTestBase {

  static {
    setTestDriver(new GoraSqlTestDriver());
  }

  public TestSqlStore() {
  }

  @Override
  protected DataStore<String, Employee> createEmployeeDataStore() throws IOException {
    return null;
  }

  @Override
  protected DataStore<String, WebPage> createWebPageDataStore() {
    return null;
  }

  
  public static void main(String[] args) throws IOException {
    new TestSqlStore().testDeleteByQuery();
  }
}
