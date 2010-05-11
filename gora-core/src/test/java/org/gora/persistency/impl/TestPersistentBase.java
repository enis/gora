
package org.gora.persistency.impl;

import org.gora.example.generated.Employee;
import org.gora.example.generated.WebPage;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testcase for PersistentBase class
 */
public class TestPersistentBase {

  private static final String[] WEBPAGE_FIELDS = {"url", "content", 
    "parsedContent", "outlinks"};
  
  
  private static final String[] EMPLOYEE_FIELDS = {"name","dateOfBirth","ssn","salary",};
  @Test
  public void testGetFields() {
    WebPage page = new WebPage();
    String[] fields = page.getFields();
    Assert.assertArrayEquals(WEBPAGE_FIELDS, fields);
  }
  
  @Test
  public void testGetField() {
    WebPage page = new WebPage();
    for(int i=0; i<WEBPAGE_FIELDS.length; i++) {
      String field = page.getField(i);
      Assert.assertEquals(WEBPAGE_FIELDS[i], field);
    }
  }
  
  @Test
  public void testGetFieldIndex() {
    WebPage page = new WebPage();
    for(int i=0; i<WEBPAGE_FIELDS.length; i++) {
      int index = page.getFieldIndex(WEBPAGE_FIELDS[i]);
      Assert.assertEquals(i, index);
    }
  }
  
  @Test
  public void testFieldsWithTwoClasses() {
    WebPage page = new WebPage();
    for(int i=0; i<WEBPAGE_FIELDS.length; i++) {
      int index = page.getFieldIndex(WEBPAGE_FIELDS[i]);
      Assert.assertEquals(i, index);
    }
    Employee employee = new Employee();
    for(int i=0; i<EMPLOYEE_FIELDS.length; i++) {
      int index = employee.getFieldIndex(EMPLOYEE_FIELDS[i]);
      Assert.assertEquals(i, index);
    }
  }
}
