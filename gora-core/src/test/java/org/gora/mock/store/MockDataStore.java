
package org.gora.mock.store;

import java.io.IOException;
import java.util.List;

import org.gora.mock.persistency.MockPersistent;
import org.gora.query.PartitionQuery;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;

public class MockDataStore implements DataStore<Object, MockPersistent> {

  @Override
  public void close() throws IOException {
  }

  @Override
  public void createTable() throws IOException {
  }

  @Override
  public void delete(Object key) throws IOException {
  }

  @Override
  public Result<Object, MockPersistent> execute(
      Query<Object, MockPersistent> query) throws IOException {
    return null;
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public MockPersistent get(Object key, String[] fields) throws IOException {
    return null;
  }

  @Override
  public Class<Object> getKeyClass() {
    return null;
  }

  @Override
  public List<PartitionQuery<Object, MockPersistent>> getPartitions(
      Query<Object, MockPersistent> query) throws IOException {
    return null;
  }

  @Override
  public Class<MockPersistent> getPersistentClass() {
    return null;
  }

  @Override
  public MockPersistent newInstance() throws IOException {
    return new MockPersistent();
  }

  @Override
  public Query<Object, MockPersistent> newQuery() {
    return null;
  }

  @Override
  public void put(Object key, MockPersistent obj) throws IOException {
  }

  @Override
  public void setKeyClass(Class<Object> keyClass) {
  }

  @Override
  public void setPersistentClass(Class<MockPersistent> persistentClass) {
  }
}
