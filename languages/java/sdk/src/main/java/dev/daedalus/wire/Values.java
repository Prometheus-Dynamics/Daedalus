package dev.daedalus.wire;

import dev.daedalus.wire.Types.Ty;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Values, as the wire carries them, typed by their {@link Ty}.
 *
 * <p>A value in Java is: an {@code Integer} (u8, u16, i32), a {@code Long} (u32, i64, isize), a
 * {@code BigInteger} (u64, usize, always non-negative), a {@code Byte} (i8), a {@code Short} (i16),
 * a {@code Float} or {@code Double}, a {@code Boolean}, a {@code String}, a {@code byte[]} (bytes and
 * opaque values), {@code null} for unit or an absent optional, the value (optional), a {@code List},
 * a {@code Map} (map, and struct by field name), an {@code Object[]} (tuple), or a {@link Variant}
 * (enum). A value that does not have the shape of its type is refused with
 * {@link IllegalArgumentException}.
 */
public final class Values {
    public static final int MAX_DEPTH = 32;
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    /** An enum value: the variant's name and its payload, or null. */
    public record Variant(String name, Object payload) {}

    private Values() {}

    /**
     * The codecs one side has (the opaque type keys it can read and write), and, for a worker,
     * the ports its nodes declare. A value of an opaque type with no codec is refused.
     */
    public static final class Schema {
        private final Map<String, Ty> ports = new java.util.HashMap<>();
        private final Set<String> codecs;

        public Schema(Set<String> codecs) {
            this.codecs = codecs;
        }

        public void port(String node, String direction, String name, Ty ty) {
            ports.put(node + "\u0000" + direction + "\u0000" + name, ty);
        }

        public Ty lookup(String node, String direction, String name) {
            Ty ty = ports.get(node + "\u0000" + direction + "\u0000" + name);
            if (ty == null) throw new IllegalArgumentException("node " + node + " has no " + direction + " " + name);
            return ty;
        }

        public boolean hasCodec(String key) { return codecs.contains(key); }
    }

    public static void encode(Ty ty, Object value, Schema schema, Bytes.Writer out, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("value nests too deep");
        switch (ty.kind) {
            case "scalar" -> encodeScalar(ty.scalar, value, out);
            case "opaque" -> {
                if (!(value instanceof byte[] bytes)) throw new IllegalArgumentException("an opaque value is bytes");
                if (!schema.hasCodec(ty.key)) throw new IllegalArgumentException("type " + ty.key + " has no codec");
                out.bytes(bytes);
            }
            case "optional" -> {
                if (value == null) out.u8(0);
                else {
                    out.u8(1);
                    encode(ty.items.get(0), value, schema, out, depth + 1);
                }
            }
            case "list" -> {
                List<?> items = (List<?>) value;
                out.len(items.size());
                for (Object item : items) encode(ty.items.get(0), item, schema, out, depth + 1);
            }
            case "map" -> {
                Map<?, ?> map = (Map<?, ?>) value;
                out.len(map.size());
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    encode(ty.items.get(0), e.getKey(), schema, out, depth + 1);
                    encode(ty.items.get(1), e.getValue(), schema, out, depth + 1);
                }
            }
            case "tuple" -> {
                Object[] items = (Object[]) value;
                if (items.length != ty.items.size()) throw new IllegalArgumentException("a tuple has the wrong length");
                for (int i = 0; i < items.length; i++) encode(ty.items.get(i), items[i], schema, out, depth + 1);
            }
            case "struct" -> {
                Map<?, ?> fields = (Map<?, ?>) value;
                for (int i = 0; i < ty.items.size(); i++) {
                    String name = ty.names.get(i);
                    if (!fields.containsKey(name)) throw new IllegalArgumentException("struct has no field " + name);
                    encode(ty.items.get(i), fields.get(name), schema, out, depth + 1);
                }
            }
            case "enum" -> {
                Variant variant = (Variant) value;
                int index = ty.names.indexOf(variant.name());
                if (index < 0) throw new IllegalArgumentException("no variant " + variant.name());
                out.len(index);
                Ty payload = ty.payloads.get(index);
                if (payload != null) encode(payload, variant.payload(), schema, out, depth + 1);
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        }
    }

    public static Object decode(Ty ty, Bytes.Reader in, Schema schema, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("value nests too deep");
        return switch (ty.kind) {
            case "scalar" -> decodeScalar(ty.scalar, in);
            case "opaque" -> {
                if (!schema.hasCodec(ty.key)) throw new IllegalArgumentException("type " + ty.key + " has no codec");
                yield in.bytes();
            }
            case "optional" -> {
                int flag = in.u8();
                if (flag == 0) yield null;
                if (flag != 1) throw new IllegalArgumentException("bad optional flag " + flag);
                yield decode(ty.items.get(0), in, schema, depth + 1);
            }
            case "list" -> {
                int n = in.count();
                List<Object> items = new ArrayList<>(n);
                for (int i = 0; i < n; i++) items.add(decode(ty.items.get(0), in, schema, depth + 1));
                yield items;
            }
            case "map" -> {
                int n = in.count();
                Map<Object, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    Object key = decode(ty.items.get(0), in, schema, depth + 1);
                    out.put(key, decode(ty.items.get(1), in, schema, depth + 1));
                }
                yield out;
            }
            case "tuple" -> {
                Object[] items = new Object[ty.items.size()];
                for (int i = 0; i < items.length; i++) items[i] = decode(ty.items.get(i), in, schema, depth + 1);
                yield items;
            }
            case "struct" -> {
                Map<String, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < ty.items.size(); i++) {
                    out.put(ty.names.get(i), decode(ty.items.get(i), in, schema, depth + 1));
                }
                yield out;
            }
            case "enum" -> {
                long index = in.u32();
                if (index >= ty.names.size()) throw new IllegalArgumentException("bad variant " + index);
                int i = (int) index;
                Ty payload = ty.payloads.get(i);
                yield new Variant(ty.names.get(i), payload == null ? null : decode(payload, in, schema, depth + 1));
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        };
    }

    private static void encodeScalar(String name, Object value, Bytes.Writer out) {
        switch (name) {
            case "unit" -> { }
            case "bool" -> out.bool((Boolean) value);
            case "i8" -> out.i8(((Number) value).byteValue());
            case "i16" -> out.i16(((Number) value).shortValue());
            case "i32" -> out.i32(((Number) value).intValue());
            case "i64", "isize" -> out.i64(((Number) value).longValue());
            case "u8" -> out.u8(range(value, 0, 255).intValue());
            case "u16" -> out.u16(range(value, 0, 65535).intValue());
            case "u32" -> out.u32(range(value, 0, 4294967295L).longValue());
            case "u64", "usize" -> out.u64(unsigned(value).longValue());
            case "f32" -> out.f32(((Number) value).floatValue());
            case "f64" -> out.f64(((Number) value).doubleValue());
            case "string" -> out.str((String) value);
            case "bytes" -> out.bytes((byte[]) value);
            default -> throw new IllegalArgumentException("unknown scalar " + name);
        }
    }

    private static Object decodeScalar(String name, Bytes.Reader in) {
        return switch (name) {
            case "unit" -> null;
            case "bool" -> in.bool();
            case "i8" -> in.i8();
            case "i16" -> in.i16();
            case "i32" -> in.i32();
            case "i64", "isize" -> in.i64();
            case "u8" -> in.u8();
            case "u16" -> in.u16();
            case "u32" -> in.u32();
            case "u64", "usize" -> new BigInteger(Long.toUnsignedString(in.u64()));
            case "f32" -> in.f32();
            case "f64" -> in.f64();
            case "string" -> in.str();
            case "bytes" -> in.bytes();
            default -> throw new IllegalArgumentException("unknown scalar " + name);
        };
    }

    private static BigInteger range(Object value, long low, long high) {
        BigInteger v = BigInteger.valueOf(((Number) value).longValue());
        if (v.compareTo(BigInteger.valueOf(low)) < 0 || v.compareTo(BigInteger.valueOf(high)) > 0) {
            throw new IllegalArgumentException(v + " is outside the range");
        }
        return v;
    }

    /** An unsigned 64-bit value: a BigInteger in [0, 2^64), or a long read as unsigned. */
    private static BigInteger unsigned(Object value) {
        BigInteger v = value instanceof BigInteger big ? big : BigInteger.valueOf(((Number) value).longValue());
        if (v.signum() < 0 || v.compareTo(U64_MAX) > 0) throw new IllegalArgumentException(v + " is outside u64");
        return v;
    }
}
