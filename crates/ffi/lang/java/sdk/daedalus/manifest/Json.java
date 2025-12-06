package daedalus.manifest;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class Json {
  private Json() {}

  public static String stringify(Object v) {
    StringBuilder sb = new StringBuilder();
    write(sb, v);
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private static void write(StringBuilder sb, Object v) {
    if (v == null) {
      sb.append("null");
      return;
    }
    if (v instanceof String) {
      sb.append('"');
      escape(sb, (String) v);
      sb.append('"');
      return;
    }
    if (v instanceof Number || v instanceof Boolean) {
      sb.append(v.toString());
      return;
    }
    if (v instanceof Enum) {
      sb.append('"');
      escape(sb, v.toString());
      sb.append('"');
      return;
    }
    if (v instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) v;
      sb.append('{');
      Iterator<Map.Entry<String, Object>> it = m.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, Object> e = it.next();
        sb.append('"');
        escape(sb, e.getKey());
        sb.append('"').append(':');
        write(sb, e.getValue());
        if (it.hasNext()) sb.append(',');
      }
      sb.append('}');
      return;
    }
    if (v instanceof List) {
      List<Object> arr = (List<Object>) v;
      sb.append('[');
      for (int i = 0; i < arr.size(); i++) {
        if (i > 0) sb.append(',');
        write(sb, arr.get(i));
      }
      sb.append(']');
      return;
    }
    if (v.getClass().isArray()) {
      Object[] arr = (Object[]) v;
      sb.append('[');
      for (int i = 0; i < arr.length; i++) {
        if (i > 0) sb.append(',');
        write(sb, arr[i]);
      }
      sb.append(']');
      return;
    }
    sb.append('"');
    escape(sb, String.valueOf(v));
    sb.append('"');
  }

  private static void escape(StringBuilder sb, String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 32) sb.append(String.format("\\u%04x", (int) c));
          else sb.append(c);
      }
    }
  }
}

