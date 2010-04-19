
package org.gora.hbase.query;

import org.gora.Persistent;
import org.gora.query.impl.QueryBase;
import org.gora.store.DataStore;

public class HBaseQuery<K, T extends Persistent> extends QueryBase<K, T> {

  public HBaseQuery(DataStore<K, T> dataStore) {
    super(dataStore);
  }

}
