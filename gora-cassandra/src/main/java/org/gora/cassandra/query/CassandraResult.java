package org.gora.cassandra.query;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.gora.cassandra.client.CassandraClient;
import org.gora.cassandra.client.Row;
import org.gora.cassandra.client.Select;
import org.gora.cassandra.store.CassandraStore;
import org.gora.persistency.Persistent;
import org.gora.query.Query;
import org.gora.query.impl.ResultBase;
import org.gora.store.DataStore;

public class CassandraResult<K, T extends Persistent>
extends ResultBase<K, T> {

  private Iterator<Row> rowIter;

  private CassandraStore<K, T> store;

  private String[] fields;

  public CassandraResult(DataStore<K, T> dataStore, Query<K, T> query,
      int batchRowCount) throws IOException {
    super(dataStore, query);

    store = (CassandraStore<K, T>) dataStore;
    fields = query.getFields();

    boolean isUsingTokens = (query instanceof CassandraPartitionQuery);
    String startTokenOrKey;
    String endTokenOrKey;

    if (isUsingTokens) {
      CassandraPartitionQuery<K, T> partitionQuery = (CassandraPartitionQuery<K, T>) query;
      startTokenOrKey = partitionQuery.getStartToken();
      endTokenOrKey = partitionQuery.getEndToken();
    } else {
      CassandraQuery<K, T> cassandraQuery = (CassandraQuery<K, T>) query;
      startTokenOrKey = cassandraQuery.getStartKey().toString();
      endTokenOrKey = cassandraQuery.getEndKey().toString();
    }

    Select select = store.createSelect(fields);

    CassandraClient client = store.getClientByLocation(getLocation(query));
    if (isUsingTokens) {
      rowIter =
        client.getTokenRange(startTokenOrKey, endTokenOrKey,
            batchRowCount, select).iterator();
    } else {
      rowIter = client.getRange(startTokenOrKey, endTokenOrKey,
          batchRowCount, select).iterator();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  protected boolean nextInner() throws IOException {
    if (!rowIter.hasNext()) {
      return false;
    }
    Row row = rowIter.next();
    if (row == null) {
      return false;
    }

    key = toKey(row.getKey());
    persistent = store.newInstance(row, fields);

    return true;
  }

  @SuppressWarnings("unchecked")
  private K toKey(String keyStr) {
    Class<K> keyClass = dataStore.getKeyClass();
    if (keyClass.isAssignableFrom(String.class)) {
      return (K) keyStr;
    }
    if (keyClass.isAssignableFrom(Integer.class)) {
      return (K) (Integer) Integer.parseInt(keyStr);
    }
    if (keyClass.isAssignableFrom(Float.class)) {
      return (K) (Float) Float.parseFloat(keyStr);
    }
    if (keyClass.isAssignableFrom(Double.class)) {
      return (K) (Double) Double.parseDouble(keyStr);
    }
    if (keyClass.isAssignableFrom(Long.class)) {
      return (K) (Long) Long.parseLong(keyStr);
    }
    if (keyClass.isAssignableFrom(Short.class)) {
      return (K) (Short) Short.parseShort(keyStr);
    }
    if (keyClass.isAssignableFrom(Byte.class)) {
      return (K) (Byte) Byte.parseByte(keyStr);
    }

    throw new RuntimeException("Can't parse " + keyStr +
                               " as an instance of " + keyClass);
  }

  @Override
  public void close() throws IOException { }

  private String getLocation(Query<K, T> query) {
    if (!(query instanceof CassandraPartitionQuery)) {
      return null;
    }
    CassandraPartitionQuery<K, T> partitonQuery =
      (CassandraPartitionQuery<K, T>) query;
    InetAddress[] localAddresses = new InetAddress[0];
    try {
      localAddresses = InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      throw new AssertionError(e);
    }
    for (InetAddress address : localAddresses) {
      for (String location : partitonQuery.getEndPoints()) {
        InetAddress locationAddress = null;
        try {
          locationAddress = InetAddress.getByName(location);
        } catch (UnknownHostException e) {
          throw new AssertionError(e);
        }
        if (address.equals(locationAddress)) {
          return location;
        }
      }
    }
    return partitonQuery.getEndPoints()[0];
  }
}