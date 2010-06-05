
package org.gora.sql.store;

import java.io.IOException;

import org.gora.example.generated.Employee;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStoreFactory;

public class Main {

  public static void main(String[] args) throws IOException {

    SqlStore<String, Employee> store = DataStoreFactory.getDataStore(
        SqlStore.class, String.class, Employee.class);
    
    //Employee employee = DataStoreTestUtil.createEmployee(store);
    
    //store.put(employee.getSsn().toString(), employee);
    //store.flush();
    
    //store.close();
    
    Query<String, Employee> query = store.newQuery();
    
    query.setKey("101010101010");
    
    Result<String, Employee> result = query.execute();
    
    while(result.next()) {
      Employee employee = result.get();
      System.out.println("-------");
      System.out.println(employee.getName());
      System.out.println(employee.getSsn());
      System.out.println(employee.getDateOfBirth());
      System.out.println(employee.getSalary());  
    }
  }
  
}
