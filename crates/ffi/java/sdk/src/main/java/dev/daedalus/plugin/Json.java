package dev.daedalus.plugin;

import java.util.List;
import java.util.Map;

final class Json {
  private Json() {}

  static String write(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof String string) {
      return quote(string);
    }
    if (value instanceof Number || value instanceof Boolean) {
      return value.toString();
    }
    if (value instanceof Map<?, ?> map) {
      StringBuilder out = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) {
          out.append(",");
        }
        first = false;
        out.append(quote(String.valueOf(entry.getKey()))).append(":").append(write(entry.getValue()));
      }
      return out.append("}").toString();
    }
    if (value instanceof List<?> list) {
      StringBuilder out = new StringBuilder("[");
      for (int i = 0; i < list.size(); i++) {
        if (i > 0) {
          out.append(",");
        }
        out.append(write(list.get(i)));
      }
      return out.append("]").toString();
    }
    throw new IllegalArgumentException("unsupported JSON value: " + value.getClass().getName());
  }

  private static String quote(String value) {
    StringBuilder out = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> out.append(ch);
      }
    }
    return out.append('"').toString();
  }
}
