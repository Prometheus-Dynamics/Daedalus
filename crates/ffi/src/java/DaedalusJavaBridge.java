
import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DaedalusJavaBridge {
  // Minimal JSON parser/emitter (object/array/string/number/bool/null).
  static final class Json {
    static Object parse(String s) {
      return new Parser(s).parseValue();
    }
    static String stringify(Object v) {
      StringBuilder sb = new StringBuilder();
      write(sb, v);
      return sb.toString();
    }
    static void write(StringBuilder sb, Object v) {
      if (v == null) { sb.append("null"); return; }
      if (v instanceof Boolean) { sb.append(((Boolean)v) ? "true" : "false"); return; }
      if (v instanceof Number) { sb.append(v.toString()); return; }
      if (v instanceof String) { sb.append('\"'); escape(sb, (String)v); sb.append('\"'); return; }
      if (v instanceof Optional) {
        Optional<?> opt = (Optional<?>) v;
        write(sb, opt.isPresent() ? opt.get() : null);
        return;
      }
      if (v.getClass().isRecord()) {
        try {
          RecordComponent[] comps = v.getClass().getRecordComponents();
          // Heuristic: treat single-field records named `value` that implement an interface as
          // enum-like variants and encode as {"name": "...", "value": ...}.
          if (comps.length == 1 && "value".equals(comps[0].getName()) && v.getClass().getInterfaces().length > 0) {
            Map<String,Object> ev = new LinkedHashMap<>();
            ev.put("name", v.getClass().getSimpleName());
            ev.put("value", comps[0].getAccessor().invoke(v));
            write(sb, ev);
            return;
          }
          Map<String,Object> m = new LinkedHashMap<>();
          for (RecordComponent c : comps) {
            m.put(c.getName(), c.getAccessor().invoke(v));
          }
          write(sb, m);
          return;
        } catch (Throwable t) {
          // Fall through to string fallback.
        }
      }
      if (v instanceof Map) {
        sb.append('{');
        boolean first = true;
        for (Object eObj : ((Map<?,?>)v).entrySet()) {
          Map.Entry<?,?> e = (Map.Entry<?,?>)eObj;
          if (!first) sb.append(',');
          first = false;
          sb.append('\"'); escape(sb, String.valueOf(e.getKey())); sb.append('\"');
          sb.append(':');
          write(sb, e.getValue());
        }
        sb.append('}');
        return;
      }
      if (v instanceof List) {
        sb.append('[');
        boolean first = true;
        for (Object it : (List<?>)v) {
          if (!first) sb.append(',');
          first = false;
          write(sb, it);
        }
        sb.append(']');
        return;
      }
      // Fallback: toString as JSON string.
      sb.append('\"'); escape(sb, String.valueOf(v)); sb.append('\"');
    }
    static void escape(StringBuilder sb, String s) {
      for (int i=0;i<s.length();i++) {
        char c = s.charAt(i);
        switch(c) {
          case '\"': sb.append("\\\""); break;
          case '\\': sb.append("\\\\"); break;
          case '\n': sb.append("\\n"); break;
          case '\r': sb.append("\\r"); break;
          case '\t': sb.append("\\t"); break;
          default:
            if (c < 0x20) {
              sb.append(String.format("\\u%04x", (int)c));
            } else sb.append(c);
        }
      }
    }
    static final class Parser {
      final String s;
      int i=0;
      Parser(String s){ this.s=s; }
      void ws(){ while(i<s.length()){ char c=s.charAt(i); if(c==' '||c=='\n'||c=='\r'||c=='\t') i++; else break; } }
      Object parseValue(){
        ws();
        if(i>=s.length()) return null;
        char c=s.charAt(i);
        if(c=='{') return parseObj();
        if(c=='[') return parseArr();
        if(c=='\"') return parseStr();
        if(c=='t' && s.startsWith("true", i)){ i+=4; return Boolean.TRUE; }
        if(c=='f' && s.startsWith("false", i)){ i+=5; return Boolean.FALSE; }
        if(c=='n' && s.startsWith("null", i)){ i+=4; return null; }
        return parseNum();
      }
      Map<String,Object> parseObj(){
        Map<String,Object> m=new LinkedHashMap<>();
        i++; ws();
        if(i<s.length() && s.charAt(i)=='}'){ i++; return m; }
        while(true){
          ws();
          String k=parseStr();
          ws(); expect(':');
          Object v=parseValue();
          m.put(k,v);
          ws();
          if(peek('}')){ i++; break; }
          expect(',');
        }
        return m;
      }
      List<Object> parseArr(){
        List<Object> a=new ArrayList<>();
        i++; ws();
        if(i<s.length() && s.charAt(i)==']'){ i++; return a; }
        while(true){
          Object v=parseValue();
          a.add(v);
          ws();
          if(peek(']')){ i++; break; }
          expect(',');
        }
        return a;
      }
      String parseStr(){
        expect('\"');
        StringBuilder sb=new StringBuilder();
        while(i<s.length()){
          char c=s.charAt(i++);
          if(c=='\"') break;
          if(c=='\\'){
            if(i>=s.length()) break;
            char e=s.charAt(i++);
            switch(e){
              case '\"': sb.append('\"'); break;
              case '\\': sb.append('\\'); break;
              case '/': sb.append('/'); break;
              case 'b': sb.append('\b'); break;
              case 'f': sb.append('\f'); break;
              case 'n': sb.append('\n'); break;
              case 'r': sb.append('\r'); break;
              case 't': sb.append('\t'); break;
              case 'u':
                String hex=s.substring(i,i+4); i+=4;
                sb.append((char)Integer.parseInt(hex,16));
                break;
              default: sb.append(e);
            }
          } else sb.append(c);
        }
        return sb.toString();
      }
      Number parseNum(){
        int j=i;
        while(i<s.length()){
          char c=s.charAt(i);
          if((c>='0'&&c<='9')||c=='-'||c=='+'||c=='.'||c=='e'||c=='E'){ i++; continue; }
          break;
        }
        String sub=s.substring(j,i);
        try{
          if(sub.contains(".")||sub.contains("e")||sub.contains("E")) return Double.parseDouble(sub);
          long v=Long.parseLong(sub);
          if(v>=Integer.MIN_VALUE && v<=Integer.MAX_VALUE) return (int)v;
          return v;
        }catch(Exception ex){
          return 0;
        }
      }
      boolean peek(char c){ return i<s.length() && s.charAt(i)==c; }
      void expect(char c){
        ws();
        if(i>=s.length() || s.charAt(i)!=c) throw new RuntimeException("expected '"+c+"' at "+i);
        i++;
      }
    }
  }

  static final class RawIoProxy implements InvocationHandler {
    public final List<Map<String,Object>> events = new ArrayList<>();
    public void push(String port, Object value) {
      if (port == null || port.isEmpty()) throw new IllegalArgumentException("io.push requires a port");
      Map<String,Object> ev = new LinkedHashMap<>();
      ev.put("port", port);
      ev.put("value", value);
      events.add(ev);
    }
    @Override public Object invoke(Object proxy, Method method, Object[] args) {
      String name = method.getName();
      if ("push".equals(name)) {
        push(String.valueOf(args[0]), args[1]);
        return null;
      }
      if ("pushMany".equals(name)) {
        Object port = args[0];
        Object values = args[1];
        if (values instanceof List) {
          for (Object v : (List<?>) values) push(String.valueOf(port), v);
        }
        return null;
      }
      if ("events".equals(name)) return events;
      return null;
    }
  }

  static Object tryMakeRawIoProxy(RawIoProxy handler) {
    try {
      Class<?> rawIo = Class.forName("daedalus.bridge.RawIo");
      return Proxy.newProxyInstance(rawIo.getClassLoader(), new Class<?>[]{rawIo}, handler);
    } catch (Throwable t) {
      return null;
    }
  }

  static Object tryMakeExtra(Map<String,Object> ctx, Map<String,Object> node, Object io) {
    try {
      Class<?> extra = Class.forName("daedalus.bridge.Extra");
      for (Constructor<?> c : extra.getConstructors()) {
        Class<?>[] pts = c.getParameterTypes();
        if (pts.length == 3 && Map.class.isAssignableFrom(pts[0]) && Map.class.isAssignableFrom(pts[1])) {
          return c.newInstance(ctx, node, io);
        }
      }
    } catch (Throwable t) {
      // ignore
    }
    return null;
  }

  static boolean isExtraParam(Parameter p) {
    return "daedalus.bridge.Extra".equals(p.getType().getName());
  }

  static Object coerce(Object v, Class<?> t) throws Exception {
    if (v == null) return null;
    if (t == Object.class) return v;
    if (t == String.class) return String.valueOf(v);
    if (t == boolean.class || t == Boolean.class) {
      if (v instanceof Boolean) return v;
      if (v instanceof Number) return ((Number) v).intValue() != 0;
      return Boolean.parseBoolean(String.valueOf(v));
    }
    if (t == int.class || t == Integer.class) {
      if (v instanceof Number) return ((Number) v).intValue();
      return Integer.parseInt(String.valueOf(v));
    }
    if (t == long.class || t == Long.class) {
      if (v instanceof Number) return ((Number) v).longValue();
      return Long.parseLong(String.valueOf(v));
    }
    if (t == float.class || t == Float.class) {
      if (v instanceof Number) return ((Number) v).floatValue();
      return Float.parseFloat(String.valueOf(v));
    }
    if (t == double.class || t == Double.class) {
      if (v instanceof Number) return ((Number) v).doubleValue();
      return Double.parseDouble(String.valueOf(v));
    }
    if (t == Optional.class) {
      return Optional.ofNullable(v);
    }
    if (t.isRecord() && v instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String,Object> m = (Map<String,Object>) v;
      RecordComponent[] comps = t.getRecordComponents();
      Class<?>[] ctorTypes = new Class<?>[comps.length];
      Object[] ctorArgs = new Object[comps.length];
      for (int i=0;i<comps.length;i++) {
        RecordComponent c = comps[i];
        ctorTypes[i] = c.getType();
        ctorArgs[i] = coerce(m.get(c.getName()), c.getType());
      }
      Constructor<?> ctor = t.getDeclaredConstructor(ctorTypes);
      return ctor.newInstance(ctorArgs);
    }
    return v;
  }

  static Object[] buildArgs(Method m, List<Object> args, Object state, Object extra) throws Exception {
    Parameter[] ps = m.getParameters();
    Object[] out = new Object[ps.length];
    int ai = 0;
    for (int i=0;i<ps.length;i++) {
      Parameter p = ps[i];
      if (isExtraParam(p)) {
        out[i] = extra;
        continue;
      }
      if (state != null && "state".equals(p.getName())) {
        out[i] = coerce(state, p.getType());
        continue;
      }
      if (ai >= args.size()) return null;
      out[i] = coerce(args.get(ai), p.getType());
      ai++;
    }
    if (ai != args.size()) return null;
    return out;
  }

  static Object invokeStateless(Class<?> cls, String method, List<Object> args, Object extra) throws Exception {
    for (Method m : cls.getMethods()) {
      if (!m.getName().equals(method)) continue;
      Object[] call = buildArgs(m, args, null, extra);
      if (call == null) continue;
      return m.invoke(null, call);
    }
    throw new RuntimeException("missing stateless method "+method);
  }

  static Object invokeStateful(Class<?> cls, String method, List<Object> args, Object state, Object stateSpec, Object extra) throws Exception {
    // Prefer a single-parameter `daedalus.bridge.StatefulInvocation`.
    try {
      Class<?> invCls = Class.forName("daedalus.bridge.StatefulInvocation");
      Constructor<?> ctor = null;
      for (Constructor<?> c : invCls.getConstructors()) {
        Class<?>[] pts = c.getParameterTypes();
        if (pts.length == 4 && List.class.isAssignableFrom(pts[0])) { ctor = c; break; }
      }
      Object inv = (ctor != null) ? ctor.newInstance(args, state, stateSpec, extra) : null;
      if (inv != null) {
        for (Method m : cls.getMethods()) {
          if (!m.getName().equals(method)) continue;
          Class<?>[] pts = m.getParameterTypes();
          if (pts.length == 1 && invCls.isAssignableFrom(pts[0])) {
            return m.invoke(null, inv);
          }
        }
      }
    } catch (Throwable t) {
      // ignore, fall back to direct args+state binding
    }

    for (Method m : cls.getMethods()) {
      if (!m.getName().equals(method)) continue;
      Object[] call = buildArgs(m, args, state, extra);
      if (call == null) continue;
      return m.invoke(null, call);
    }
    throw new RuntimeException("missing stateful method "+method);
  }

  static Map<String,Object> stateResultToMap(Object sr) {
    try {
      Class<?> cls = sr.getClass();
      if (!"daedalus.bridge.StateResult".equals(cls.getName())) return null;
      Field fs = cls.getField("state");
      Field fo = cls.getField("outputs");
      Map<String,Object> out = new LinkedHashMap<>();
      out.put("state", fs.get(sr));
      out.put("outputs", fo.get(sr));
      return out;
    } catch (Throwable t) {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while((n=System.in.read(buf))>0){ baos.write(buf,0,n); }
    String in = baos.toString(StandardCharsets.UTF_8);
    Object parsed = Json.parse(in);
    if(!(parsed instanceof Map)) throw new RuntimeException("expected json object payload");
    @SuppressWarnings("unchecked")
    Map<String,Object> data = (Map<String,Object>)parsed;

    String clsName = String.valueOf(data.get("class"));
    String method = String.valueOf(data.get("method"));
    Object argsObj = data.get("args");
    @SuppressWarnings("unchecked")
    List<Object> callArgs = (argsObj instanceof List) ? (List<Object>)argsObj : new ArrayList<>();
    boolean stateful = Boolean.TRUE.equals(data.get("stateful"));
    boolean rawIo = Boolean.TRUE.equals(data.get("raw_io"));

    @SuppressWarnings("unchecked")
    Map<String,Object> ctx = (Map<String,Object>)data.get("ctx");
    @SuppressWarnings("unchecked")
    Map<String,Object> node = (Map<String,Object>)data.get("node");

    RawIoProxy ioHandler = rawIo ? new RawIoProxy() : null;
    Object io = (rawIo && ioHandler != null) ? tryMakeRawIoProxy(ioHandler) : null;
    Object extra = tryMakeExtra(ctx, node, io);

    Class<?> cls = Class.forName(clsName);
    Object result;
    if (!stateful) {
      result = invokeStateless(cls, method, callArgs, extra);
      if (rawIo && ioHandler != null && !ioHandler.events.isEmpty()) {
        Map<String,Object> out = new LinkedHashMap<>();
        out.put("events", ioHandler.events);
        System.out.write(Json.stringify(out).getBytes(StandardCharsets.UTF_8));
        return;
      }
      if (rawIo) {
        Map<String,Object> out = new LinkedHashMap<>();
        out.put("outputs", result);
        System.out.write(Json.stringify(out).getBytes(StandardCharsets.UTF_8));
        return;
      }
      System.out.write(Json.stringify(result).getBytes(StandardCharsets.UTF_8));
      return;
    }

    Object state = data.get("state");
    Object stateSpec = data.get("state_spec");
    result = invokeStateful(cls, method, callArgs, state, stateSpec, extra);
    Map<String,Object> out = new LinkedHashMap<>();

    Map<String,Object> sr = (result != null) ? stateResultToMap(result) : null;
    if (sr != null) {
      out.putAll(sr);
    } else if (result instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String,Object> rm = (Map<String,Object>) result;
      out.putAll(rm);
    } else if (result instanceof List && ((List<?>)result).size()==2) {
      List<?> l = (List<?>)result;
      out.put("state", l.get(0));
      out.put("outputs", l.get(1));
    } else {
      out.put("state", state);
      out.put("outputs", result);
    }
    if (rawIo && ioHandler != null && !ioHandler.events.isEmpty()) {
      out.put("events", ioHandler.events);
      out.put("outputs", null);
    }
    System.out.write(Json.stringify(out).getBytes(StandardCharsets.UTF_8));
  }
}

