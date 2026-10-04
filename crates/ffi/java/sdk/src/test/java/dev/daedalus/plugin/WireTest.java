package dev.daedalus.plugin;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Wire round trips; prints encoder output for the Rust test to decode with serde. */
public final class WireTest {
  /** What the Rust host serializes for {@code WireValue::UInt(u64::MAX)}. */
  private static final String UINT_MAX_JSON = "{\"kind\":\"uint\",\"value\":18446744073709551615}";

  public static void main(String[] args) {
    expect(Wire.write(Wire.encode(-1L, "u64")), UINT_MAX_JSON);
    expect(Wire.decode(Wire.read(UINT_MAX_JSON)), -1L);
    expect(Long.toUnsignedString((Long) Wire.decode(Wire.read(UINT_MAX_JSON))), "18446744073709551615");
    expect(Wire.encode(5L, "u64"), Map.of("kind", "uint", "value", BigInteger.valueOf(5)));
    expect(Wire.encode(5L, null), Map.of("kind", "int", "value", 5L));
    expect(Wire.encode(BigInteger.ONE.shiftLeft(63)).get("kind"), "uint");
    expect(Wire.encode(BigInteger.valueOf(-3)), Map.of("kind", "int", "value", -3L));
    try {
      Wire.encode(BigInteger.ONE.shiftLeft(64));
      throw new AssertionError("2^64 should not encode");
    } catch (IllegalArgumentException expected) {
      // Out of range for both i64 and u64.
    }
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("items", List.of(1L, -2L));
    nested.put("label", "a\"b\n");
    nested.put("ok", true);
    nested.put("none", null);
    nested.put("ratio", 0.5);
    expect(Wire.decode(Wire.read(Wire.write(Wire.encode(nested)))), nested);
    byte[] bytes = (byte[]) Wire.decode(Wire.read(Wire.write(Wire.encode(new byte[] {1, (byte) 255}))));
    expect(bytes[1], (byte) 255);

    System.out.println(Wire.write(List.of(
        Wire.encode(-1L, "u64"), Wire.encode(5L, "u64"), Wire.encode(-3L, null),
        Wire.encode(List.of(BigInteger.ONE.shiftLeft(63))))));
  }

  private static void expect(Object actual, Object expected) {
    if (!Objects.equals(actual, expected)) {
      throw new AssertionError("expected " + expected + ", found " + actual);
    }
  }
}
