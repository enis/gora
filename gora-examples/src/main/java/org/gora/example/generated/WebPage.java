package org.gora.example.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
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
import org.gora.persistency.StatefulHashMap;
import org.gora.persistency.ListGenericArray;

@SuppressWarnings("all")
public class WebPage extends PersistentBase {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"WebPage\",\"namespace\":\"org.gora.example.generated\",\"fields\":[{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"content\",\"type\":\"bytes\"},{\"name\":\"parsedContent\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"outlinks\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");
  public static enum Field {
    URL(0,"url"),
    CONTENT(1,"content"),
    PARSED_CONTENT(2,"parsedContent"),
    OUTLINKS(3,"outlinks"),
    ;
    private int index;
    private String name;
    Field(int index, String name) {this.index=index;this.name=name;}
    public int getIndex() {return index;}
    public String getName() {return name;}
    public String toString() {return name;}
  };
  public static final String[] _ALL_FIELDS = {"url","content","parsedContent","outlinks",};
  static {
    PersistentBase.registerFields(_ALL_FIELDS);
  }
  private Utf8 url;
  private ByteBuffer content;
  private GenericArray<Utf8> parsedContent;
  private Map<Utf8,Utf8> outlinks;
  public WebPage() {
    this(new StateManagerImpl());
  }
  public WebPage(StateManager stateManager) {
    super(stateManager);
  }
  public WebPage newInstance(StateManager stateManager) {
    return new WebPage(stateManager);
  }
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return url;
    case 1: return content;
    case 2: return parsedContent;
    case 3: return outlinks;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    getStateManager().setDirty(this, _field);
    switch (_field) {
    case 0:url = (Utf8)_value; break;
    case 1:content = (ByteBuffer)_value; break;
    case 2:parsedContent = (GenericArray<Utf8>)_value; break;
    case 3:outlinks = (Map<Utf8,Utf8>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  public Utf8 getUrl() {
    return (Utf8) get(0);
  }
  public void setUrl(Utf8 value) {
    set(0, value);
  }
  public ByteBuffer getContent() {
    return (ByteBuffer) get(1);
  }
  public void setContent(ByteBuffer value) {
    set(1, value);
  }
  public GenericArray<Utf8> getParsedContent() {
    return (GenericArray<Utf8>) get(2);
  }
  public void addToParsedContent(Utf8 element) {
    if (parsedContent == null) {
      parsedContent = new ListGenericArray<Utf8>(getSchema());
    }
    getStateManager().setDirty(this, 2);
    parsedContent.add(element);
  }
  public Map<Utf8, Utf8> getOutlinks() {
    return (Map<Utf8, Utf8>) get(3);
  }
  public Utf8 getFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    return outlinks.get(key);
  }
  public void putToOutlinks(Utf8 key, Utf8 value) {
    if (outlinks == null) {
      outlinks = new StatefulHashMap<Utf8,Utf8>();
    }
    getStateManager().setDirty(this, 3);
    outlinks.put(key, value);
  }
  public Utf8 removeFromOutlinks(Utf8 key) {
    if (outlinks == null) { return null; }
    getStateManager().setDirty(this, 3);
    return outlinks.remove(key);
  }
}
