package org.gora.sql.statement;

/**
 * A WHERE clause in an SQL statement
 */
public class WhereClause {

  private StringBuilder builder;

  public WhereClause() {
    builder = new StringBuilder();
  }

  public WhereClause(String where) {
    builder = new StringBuilder(where == null ? "" : where);
  }

  /** Adds a part to the Where clause connected with AND */
  public void addPart(String part) {
    if (builder.length() > 0) {
      builder.append(" AND ");
    }
    builder.append(part);
  }

  public void addEqualsPart(String name, String value) {
    addPart(name + " = " + value);
  }

  public void addLessThanPart(String name, String value) {
    addPart(name + " < " + value);
  }
  
  public void addLessThanEqPart(String name, String value) {
    addPart(name + " <= " + value);
  }
  
  public void addGreaterThanPart(String name, String value) {
    addPart(name + " > " + value);
  }
  
  public void addGreaterThanEqPart(String name, String value) {
    addPart(name + " >= " + value);
  }
  
  @Override
  public String toString() {
    return builder.toString();
  }
}
