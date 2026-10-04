package dev.daedalus.plugin;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Json {
  private final String text;
  private int at;

  private Json(String text) {
    this.text = text;
  }

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

  /**
   * Parses JSON into maps, lists, strings, booleans, null, {@code Double}, and integers as
   * {@code Long} or, beyond its range, {@code BigInteger}.
   */
  static Object read(String text) {
    Json json = new Json(text);
    Object value = json.value();
    json.skipSpace();
    if (json.at != text.length()) {
      throw json.error("trailing characters");
    }
    return value;
  }

  private Object value() {
    skipSpace();
    if (at >= text.length()) {
      throw error("unexpected end");
    }
    char ch = text.charAt(at);
    if (ch == '{') {
      Map<String, Object> map = new LinkedHashMap<>();
      at++;
      if (!consume('}')) {
        do {
          skipSpace();
          String key = string();
          skipSpace();
          expect(':');
          map.put(key, value());
          skipSpace();
        } while (consume(','));
        expect('}');
      }
      return map;
    }
    if (ch == '[') {
      List<Object> list = new ArrayList<>();
      at++;
      if (!consume(']')) {
        do {
          list.add(value());
          skipSpace();
        } while (consume(','));
        expect(']');
      }
      return list;
    }
    if (ch == '"') {
      return string();
    }
    for (String literal : new String[] {"true", "false", "null"}) {
      if (text.startsWith(literal, at)) {
        at += literal.length();
        return literal.equals("null") ? null : Boolean.valueOf(literal);
      }
    }
    int start = at;
    while (at < text.length() && "+-0123456789.eE".indexOf(text.charAt(at)) >= 0) {
      at++;
    }
    String number = text.substring(start, at);
    if (number.isEmpty()) {
      throw error("unexpected character");
    }
    if (number.matches("-?\\d+")) {
      BigInteger integer = new BigInteger(number);
      return integer.bitLength() < 64 ? (Object) integer.longValue() : integer;
    }
    return Double.parseDouble(number);
  }

  private String string() {
    expect('"');
    StringBuilder out = new StringBuilder();
    while (at < text.length() && text.charAt(at) != '"') {
      char ch = text.charAt(at++);
      if (ch != '\\') {
        out.append(ch);
        continue;
      }
      char escape = text.charAt(at++);
      switch (escape) {
        case 'n' -> out.append('\n');
        case 'r' -> out.append('\r');
        case 't' -> out.append('\t');
        case 'b' -> out.append('\b');
        case 'f' -> out.append('\f');
        case 'u' -> {
          out.append((char) Integer.parseInt(text.substring(at, at + 4), 16));
          at += 4;
        }
        default -> out.append(escape);
      }
    }
    expect('"');
    return out.toString();
  }

  private void skipSpace() {
    while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
      at++;
    }
  }

  private boolean consume(char ch) {
    skipSpace();
    if (at < text.length() && text.charAt(at) == ch) {
      at++;
      return true;
    }
    return false;
  }

  private void expect(char ch) {
    if (!consume(ch)) {
      throw error("expected '" + ch + "'");
    }
  }

  private IllegalArgumentException error(String message) {
    return new IllegalArgumentException("invalid JSON at " + at + ": " + message);
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
