package org.gora;

import java.util.BitSet;

import org.apache.avro.specific.SpecificRecord;

public abstract class Persistent
implements SpecificRecord {
  protected BitSet changedBits;
  protected BitSet readableBits;
  
  protected Persistent() {
    changedBits = new BitSet(getSchema().getFields().size());
    readableBits = new BitSet(getSchema().getFields().size());
  }

  public abstract boolean has(String fieldName);
  
  // TODO: None of these should not be exposed as public. Find a better way...
  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void setFieldChanged(int fieldNum) {
    changedBits.set(fieldNum);
    readableBits.set(fieldNum);
  }

  public boolean isFieldChanged(int fieldNum) {
    return changedBits.get(fieldNum);
  }

  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void setFieldReadable(int fieldNum) {
    readableBits.set(fieldNum);
  }

  public boolean isFieldReadable(int fieldNum) {
    return readableBits.get(fieldNum);
  }

  /** Do not call this method directly. This method is used internally and
   * calling this method will result in unpredictable behavior.
   */
  public void clearChangedBits() {
    changedBits.clear();
  }

  public void clearReadableBits() {
    readableBits.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpecificRecord)) return false;

    SpecificRecord r2 = (SpecificRecord)o;
    if (!this.getSchema().equals(r2.getSchema())) return false;

    int end = this.getSchema().getFields().size();
    for (int i = 0; i < end; i++) {
      Object v1 = this.get(i);
      Object v2 = r2.get(i);
      if (v1 == null) {
        if (v2 != null) return false;
      } else {
        if (!v1.equals(v2)) return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    int end = this.getSchema().getFields().size();
    for (int i = 0; i < end; i++) {
      Object o = get(i);
      result = prime * result + ((o == null) ? 0 : o.hashCode());
    }
    return result;
  }
}
