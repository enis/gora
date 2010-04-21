package org.gora.example.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.reflect.FixedSize;
import org.gora.persistency.StateManager;
import org.gora.persistency.impl.PersistentBase;
import org.gora.persistency.impl.StateManagerImpl;
import org.gora.util.StatefulHashMap;

@SuppressWarnings("all")
public class Employee extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"Employee\",\"namespace\":\"org.gora.example.generated\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"dateOfBirth\",\"type\":\"long\"},{\"name\":\"ssn\",\"type\":\"string\"},{\"name\":\"salary\",\"type\":\"int\"}]}");
  private Utf8 name;
  private long dateOfBirth;
  private Utf8 ssn;
  private int salary;
  public Employee() {
    this(new StateManagerImpl());
  }
  public Employee(StateManager stateManager) {
    super(stateManager);
  }
  public Employee newInstance(StateManager stateManager) {
    return new Employee(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return name;
    case 1: return dateOfBirth;
    case 2: return ssn;
    case 3: return salary;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:name = (Utf8)_value; break;
    case 1:dateOfBirth = (Long)_value; break;
    case 2:ssn = (Utf8)_value; break;
    case 3:salary = (Integer)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public Utf8 getName() {
    return (Utf8) get(0);
  }
  public void setName(Utf8 value) {
    set(0, value);
  }
  public long getDateOfBirth() {
    return (Long) get(1);
  }
  public void setDateOfBirth(long value) {
    set(1, value);
  }
  public Utf8 getSsn() {
    return (Utf8) get(2);
  }
  public void setSsn(Utf8 value) {
    set(2, value);
  }
  public int getSalary() {
    return (Integer) get(3);
  }
  public void setSalary(int value) {
    set(3, value);
  }
}
