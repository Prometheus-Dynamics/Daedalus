package dev.daedalus.plugin;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Daedalus {@code WireValue} JSON objects. Java has no unsigned types, so a {@code u64} travels in
 * a {@code long} holding its bits ({@code @Scalar("u64") long}): {@link #encode(long, String)}
 * writes it as {@code uint} with unsigned semantics and {@link #decode} returns the bits of a
 * {@code uint} as a {@code Long} (print it with {@link Long#toUnsignedString(long)}).
 *
 * <p>Bytes travel as base64 text ({@code "encoding": "base64"}), the form Rust sends. {@link #decode}
 * also accepts the older JSON number array ({@code "encoding": "raw"}).
 */
public final class Wire {
  private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

  private Wire() {}

  /** A {@code long} port value; {@code scalar} is the port's {@link Scalar} name, or null. */
  public static Map<String, Object> encode(long value, String scalar) {
    return "u64".equals(scalar)
        ? wire("uint", new BigInteger(Long.toUnsignedString(value)))
        : wire("int", value);
  }

  /** Any supported value; a {@code BigInteger} above {@code Long.MAX_VALUE} becomes {@code uint}. */
  public static Map<String, Object> encode(Object value) {
    if (value == null) return Map.of("kind", "unit");
    if (value instanceof Boolean bool) return wire("bool", bool);
    if (value instanceof BigInteger big) {
      if (big.bitLength() < 64) return wire("int", big.longValue());
      if (big.signum() > 0 && big.compareTo(U64_MAX) <= 0) return wire("uint", big);
      throw new IllegalArgumentException(big + " is out of range for i64 and u64");
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
      return wire("int", ((Number) value).longValue());
    }
    if (value instanceof Character ch) return wire("int", (long) ch);
    if (value instanceof Float || value instanceof Double) return wire("float", ((Number) value).doubleValue());
    if (value instanceof String string) return wire("string", string);
    if (value instanceof byte[] bytes) return wire("bytes", bytePayload(ByteBuffer.wrap(bytes)));
    if (value instanceof ByteBuffer buffer) return wire("bytes", bytePayload(buffer.duplicate()));
    if (value instanceof List<?> list) {
      List<Object> items = new ArrayList<>();
      for (Object item : list) items.add(encode(item));
      return wire("list", items);
    }
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> fields = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) fields.put(String.valueOf(entry.getKey()), encode(entry.getValue()));
      return wire("record", fields);
    }
    throw new IllegalArgumentException("no wire encoding for " + value.getClass().getName());
  }

  /**
   * The Java value of a wire object: {@code int} and {@code uint} decode to {@code Long}. A
   * {@code uint} above {@code Long.MAX_VALUE} decodes to the negative {@code long} with the same
   * bits (u64::MAX is -1L); print it with {@link Long#toUnsignedString(long)}, not as signed.
   */
  public static Object decode(Map<?, ?> wire) {
    Object value = wire.get("value");
    return switch (String.valueOf(wire.get("kind"))) {
      case "unit" -> null;
      case "bool", "float", "string" -> value;
      case "int", "uint" -> ((Number) value).longValue();
      case "bytes" -> {
        Object data = ((Map<?, ?>) value).get("data");
        if (data instanceof String base64) {
          yield Base64.getDecoder().decode(base64);
        }
        List<?> items = (List<?>) data;
        byte[] bytes = new byte[items.size()];
        for (int i = 0; i < bytes.length; i++) bytes[i] = ((Number) items.get(i)).byteValue();
        yield bytes;
      }
      case "list" -> {
        List<Object> items = new ArrayList<>();
        for (Object item : (List<?>) value) items.add(decode((Map<?, ?>) item));
        yield items;
      }
      case "record" -> {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          fields.put(String.valueOf(entry.getKey()), decode((Map<?, ?>) entry.getValue()));
        }
        yield fields;
      }
      default -> throw new IllegalArgumentException("unsupported wire value kind `" + wire.get("kind") + "`");
    };
  }

  /** JSON text of a wire value. */
  public static String write(Object wire) {
    return Json.write(wire);
  }

  /** Parses wire JSON; integers beyond {@code long} stay exact as {@code BigInteger}. */
  public static Map<?, ?> read(String json) {
    return (Map<?, ?>) Json.read(json);
  }

  private static Map<String, Object> wire(String kind, Object value) {
    Map<String, Object> wire = new LinkedHashMap<>();
    wire.put("kind", kind);
    wire.put("value", value);
    return wire;
  }

  private static Map<String, Object> bytePayload(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    // Insertion-ordered (Map.of iterates in an unspecified order), so the wire form is stable.
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("data", Base64.getEncoder().encodeToString(bytes));
    payload.put("encoding", "base64");
    return payload;
  }
}
