package org.gora.compiler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.specific.SpecificData;

/** Generate specific Java interfaces and classes for protocols and schemas. */
public class GoraCompiler {
  private File dest;
  private Writer out;
  private Set<Schema> queue = new HashSet<Schema>();

  private GoraCompiler(File dest) {
    this.dest = dest;                             // root directory for output
  }

  /** Generates Java interface and classes for a protocol.
   * @param src the source Avro protocol file
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File src, File dest) throws IOException {
    GoraCompiler compiler = new GoraCompiler(dest);
    Protocol protocol = Protocol.parse(src);
    for (Schema s : protocol.getTypes())          // enqueue types
      compiler.enqueue(s);
    compiler.compileInterface(protocol);          // generate interface
    compiler.compile();                           // generate classes for types
  }

  /** Generates Java classes for a schema. */
  public static void compileSchema(File src, File dest) throws IOException {
    GoraCompiler compiler = new GoraCompiler(dest);
    compiler.enqueue(Schema.parse(src));          // enqueue types
    compiler.compile();                           // generate classes for types
  }

  private static String camelCasify(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1);
  }

  /** Recognizes camel case */ 
  private static String toUpperCase(String s) {
    StringBuilder builder = new StringBuilder();
    
    for(int i=0; i<s.length(); i++) {
      if(i > 0) {
        if(Character.isUpperCase(s.charAt(i))
         && Character.isLowerCase(s.charAt(i-1))
         && Character.isLetter(s.charAt(i))) {
          builder.append("_");
        }
      }
      builder.append(Character.toUpperCase(s.charAt(i)));
    }
    
    return builder.toString();
  }
  
  /** Recursively enqueue schemas that need a class generated. */
  private void enqueue(Schema schema) throws IOException {
    if (queue.contains(schema)) return;
    switch (schema.getType()) {
    case RECORD:
      queue.add(schema);
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        enqueue(field.getValue());
      break;
    case MAP:
      enqueue(schema.getValueType());
      break;
    case ARRAY:
      enqueue(schema.getElementType());
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        enqueue(s);
      break;
    case ENUM:
    case FIXED:
      queue.add(schema);
      break;
    case STRING: case BYTES:
    case INT: case LONG:
    case FLOAT: case DOUBLE:
    case BOOLEAN: case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  /** Generate java classes for enqueued schemas. */
  private void compile() throws IOException {
    for (Schema schema : queue)
      compile(schema);
  }

  private void compileInterface(Protocol protocol) throws IOException {
    startFile(protocol.getName(), protocol.getNamespace());
    try {
      line(0, "public interface "+protocol.getName()+" {");

      out.append("\n");
      for (Map.Entry<String,Message> e : protocol.getMessages().entrySet()) {
        String name = e.getKey();
        Message message = e.getValue();
        Schema request = message.getRequest();
        Schema response = message.getResponse();
        line(1, unbox(response)+" "+name+"("+params(request)+")");
        line(2,"throws AvroRemoteException"+errors(message.getErrors())+";");
      }
      line(0, "}");
    } finally {
      out.close();
    }
  }

  private void startFile(String name, String space) throws IOException {
    File dir = new File(dest, space.replace('.', File.separatorChar));
    if (!dir.exists())
      if (!dir.mkdirs())
        throw new IOException("Unable to create " + dir);
    name = cap(name) + ".java";
    out = new OutputStreamWriter(new FileOutputStream(new File(dir, name)));
    header(space);
  }

  private void header(String namespace) throws IOException {
    if(namespace != null) {
      line(0, "package "+namespace+";\n");
    }
    line(0, "import java.nio.ByteBuffer;");
    line(0, "import java.util.Map;");
    line(0, "import java.util.HashMap;");
    line(0, "import org.apache.avro.Protocol;");
    line(0, "import org.apache.avro.Schema;");
    line(0, "import org.apache.avro.AvroRuntimeException;");
    line(0, "import org.apache.avro.Protocol;");
    line(0, "import org.apache.avro.util.Utf8;");
    line(0, "import org.apache.avro.ipc.AvroRemoteException;");
    line(0, "import org.apache.avro.generic.GenericArray;");
    line(0, "import org.apache.avro.specific.SpecificExceptionBase;");
    line(0, "import org.apache.avro.specific.SpecificRecordBase;");
    line(0, "import org.apache.avro.specific.SpecificRecord;");
    line(0, "import org.apache.avro.specific.SpecificFixed;");
    line(0, "import org.apache.avro.reflect.FixedSize;");
    line(0, "import org.gora.persistency.StateManager;");
    line(0, "import org.gora.persistency.impl.PersistentBase;");
    line(0, "import org.gora.persistency.impl.StateManagerImpl;");
    line(0, "import org.gora.persistency.StatefulHashMap;");
    line(0, "import org.gora.persistency.ListGenericArray;");
    for (Schema s : queue)
      if (namespace == null
          ? (s.getNamespace() != null)
          : !namespace.equals(s.getNamespace()))
        line(0, "import "+SpecificData.get().getClassName(s)+";");
    line(0, "");
    line(0, "@SuppressWarnings(\"all\")");
  }

  private String params(Schema request) throws IOException {
    StringBuilder b = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, Schema> param : request.getFieldSchemas()) {
      String paramName = param.getKey();
      b.append(unbox(param.getValue()));
      b.append(" ");
      b.append(paramName);
      if (++count < request.getFields().size())
        b.append(", ");
    }
    return b.toString();
  }

  private String errors(Schema errs) throws IOException {
    StringBuilder b = new StringBuilder();
    for (Schema error : errs.getTypes().subList(1, errs.getTypes().size())) {
      b.append(", ");
      b.append(error.getName());
    }
    return b.toString();
  }

  private void compile(Schema schema) throws IOException {
    startFile(schema.getName(), schema.getNamespace());
    try {
      switch (schema.getType()) {
      case RECORD:
        String type = type(schema);
        line(0, "public class "+ type
             +" extends PersistentBase {");
        // schema definition
        line(1, "public static final Schema _SCHEMA = Schema.parse(\""
             +esc(schema)+"\");");

        //field information
        line(1, "public static enum Field {");
        int i=0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          line(2,toUpperCase(field.getKey())+"("+(i++)+ ",\"" + field.getKey() + "\"),");
        }
        line(2, ";");
        line(2, "private int index;");
        line(2, "private String name;");
        line(2, "Field(int index, String name) {this.index=index;this.name=name;}");
        line(2, "public int getIndex() {return index;}");
        line(2, "public String getName() {return name;}");
        line(2, "public String toString() {return name;}");
        line(1, "};");
        
        StringBuilder builder = new StringBuilder(
        "public static final String[] _ALL_FIELDS = {");
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          builder.append("\"").append(field.getKey()).append("\",");
        }
        builder.append("};");
        line(1, builder.toString());
        
        line(1, "static {");
        line(2, "PersistentBase.registerFields(_ALL_FIELDS);");
        line(1, "}");
        
        // field declations
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          line(1,"private "+unbox(field.getValue())+" "+field.getKey()+";");
        }
        
        //constructors
        line(1, "public " + type + "() {");
        line(2, "this(new StateManagerImpl());");
        line(1, "}");
        line(1, "public " + type + "(StateManager stateManager) {");
        line(2, "super(stateManager);");
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          Schema fieldSchema = field.getValue();
          switch (fieldSchema.getType()) {
          case ARRAY:
            String valueType = type(fieldSchema.getElementType());
            line(2, field.getKey()+" = new ListGenericArray<"+valueType+">(getSchema());");
            break;
          case MAP:
            valueType = type(fieldSchema.getValueType());
            line(2, field.getKey()+" = new StatefulHashMap<Utf8,"+valueType+">();");
          }
        }
        line(1, "}");
        
        //newInstance(StateManager)
        line(1, "public " + type + " newInstance(StateManager stateManager) {");
        line(2, "return new " + type + "(stateManager);" );
        line(1, "}");
        
        // schema method
        line(1, "public Schema getSchema() { return _SCHEMA; }");
        // get method
        line(1, "public Object get(int _field) {");
        line(2, "switch (_field) {");
        i = 0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
          line(2, "case "+(i++)+": return "+field.getKey()+";");
        line(2, "default: throw new AvroRuntimeException(\"Bad index\");");
        line(2, "}");
        line(1, "}");
        // set method
        line(1, "@SuppressWarnings(value=\"unchecked\")");
        line(1, "public void set(int _field, Object _value) {");
        line(2, "getStateManager().setDirty(this, _field);");
        line(2, "switch (_field) {");
        i = 0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          line(2, "case "+i+":"+field.getKey()+" = ("+
               type(field.getValue())+")_value; break;");
          i++;
        }
        line(2, "default: throw new AvroRuntimeException(\"Bad index\");");
        line(2, "}");
        line(1, "}");
        
        // java bean style getters and setters
        i = 0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
          String camelKey = camelCasify(field.getKey());
          Schema fieldSchema = field.getValue();
          switch (fieldSchema.getType()) {
          case INT:case LONG:case FLOAT:case DOUBLE:
          case BOOLEAN:case BYTES:case STRING: case ENUM:
            String unboxed = unbox(fieldSchema);
            line(1, "public "+unboxed+" get" +camelKey+"() {");
            line(2, "return ("+type(field.getValue())+") get("+i+");");
            line(1, "}");
            line(1, "public void set"+camelKey+"("+unboxed+" value) {");
            line(2, "set("+i+", value);");
            line(1, "}");
            break;
          case ARRAY:
            String valueType = type(fieldSchema.getElementType());
            unboxed = unbox(fieldSchema.getElementType());
            
            line(1, "public GenericArray<"+unboxed+"> get"+camelKey+"() {");
            line(2, "return (GenericArray<"+unboxed+">) get("+i+");");
            line(1, "}");
            line(1, "public void addTo"+camelKey+"("+unboxed+" element) {");
            line(2, "getStateManager().setDirty(this, "+i+");");
            line(2, field.getKey()+".add(element);");
            line(1, "}");
            break;
          case MAP:
            valueType = type(fieldSchema.getValueType());
            unboxed = unbox(fieldSchema.getValueType());
            line(1, "public Map<Utf8, "+unboxed+"> get"+camelKey+"() {");
            line(2, "return (Map<Utf8, "+unboxed+">) get("+i+");");
            line(1, "}");
            line(1, "public "+unboxed+" getFrom"+camelKey+"(Utf8 key) {");
            line(2, "if ("+field.getKey()+" == null) { return null; }");
            line(2, "return "+field.getKey()+".get(key);");
            line(1, "}");
            line(1, "public void putTo"+camelKey+"(Utf8 key, "+unboxed+" value) {");
            line(2, "getStateManager().setDirty(this, "+i+");");
            line(2, field.getKey()+".put(key, value);");
            line(1, "}");
            line(1, "public "+unboxed+" removeFrom"+camelKey+"(Utf8 key) {");
            line(2, "if ("+field.getKey()+" == null) { return null; }");
            line(2, "getStateManager().setDirty(this, "+i+");");
            line(2, "return "+field.getKey()+".remove(key);");
            line(1, "}");
          }
          i++;
        }
        line(0, "}");
        
        break;
      case ENUM:
        line(0, "public enum "+type(schema)+" { ");
        StringBuilder b = new StringBuilder();
        int count = 0;
        for (String symbol : schema.getEnumSymbols()) {
          b.append(symbol);
          if (++count < schema.getEnumSymbols().size())
            b.append(", ");
        }
        line(1, b.toString());
        line(0, "}");
        break;
      case FIXED:
        line(0, "@FixedSize("+schema.getFixedSize()+")");
        line(0, "public class "+type(schema)+" extends SpecificFixed {}");
        break;
      case MAP: case ARRAY: case UNION: case STRING: case BYTES:
      case INT: case LONG: case FLOAT: case DOUBLE: case BOOLEAN: case NULL:
        break;
      default: throw new RuntimeException("Unknown type: "+schema);
      }
    } finally {
      out.close();
    }
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  public static String type(Schema schema) {
    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return schema.getName();
    case ARRAY:
      return "GenericArray<"+type(schema.getElementType())+">";
    case MAP:
      return "Map<Utf8,"+type(schema.getValueType())+">";
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return type(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return "Object";
    case STRING:  return "Utf8";
    case BYTES:   return "ByteBuffer";
    case INT:     return "Integer";
    case LONG:    return "Long";
    case FLOAT:   return "Float";
    case DOUBLE:  return "Double";
    case BOOLEAN: return "Boolean";
    case NULL:    return "Void";
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  public static String unbox(Schema schema) {
    switch (schema.getType()) {
    case INT:     return "int";
    case LONG:    return "long";
    case FLOAT:   return "float";
    case DOUBLE:  return "double";
    case BOOLEAN: return "boolean";
    default:      return type(schema);
    }
  }

  private void line(int indent, String text) throws IOException {
    for (int i = 0; i < indent; i ++) {
      out.append("  ");
    }
    out.append(text);
    out.append("\n");
  }

  static String cap(String name) {
    return name.substring(0,1).toUpperCase()+name.substring(1,name.length());
  }

  private static String esc(Object o) {
    return o.toString().replace("\"", "\\\"");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SpecificCompiler <schema file> <output dir>");
      System.exit(1);
    }
    compileSchema(new File(args[0]), new File(args[1]));
  }

}

