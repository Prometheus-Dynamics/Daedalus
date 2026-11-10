package dev.daedalus.worker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The worker wire protocol in Java: framing, types, values and messages.
 *
 * <p>A type is written as text: {@code i64}, {@code string}, {@code bytes}, {@code list<T>},
 * {@code optional<T>}, {@code map<K,V>}, {@code tuple<A,B>}, {@code struct{name:T,count:T}},
 * {@code enum{idle,busy:T}}, or {@code opaque(key)}. A value is an {@code Integer} (u8, u16), a
 * {@code Long} (i64, u32), a {@code BigInteger} (u64, usize), a {@code Byte} or {@code Short}
 * (i8, i16), an {@code Integer} (i32), a {@code Float} or {@code Double}, a {@code Boolean}, a
 * {@code String}, a {@code byte[]}, {@code null} or the value (optional), a {@code List}, a
 * {@code Map} (map and struct), an {@code Object[]} (tuple), a {@link Variant} (enum), or a
 * {@code byte[]} (opaque).
 */
public final class Wire {
    public static final int PROTOCOL_VERSION = 1;
    public static final int REASON_CAP = 512;
    public static final int MAX_DEPTH = 32;
    public static final long DEFAULT_CAP = 16L * 1024 * 1024;
    /** FNV-1a over the words of core/wire's LAYOUT. A hello carries it. */
    public static final long LAYOUT_HASH = 0xebbd19e914919865L;

    public static final String[] SCALARS = {
        "unit", "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize",
        "f32", "f64", "string", "bytes",
    };

    public static final Map<String, Integer> FAULT_CODES = Map.of(
        "node_error", 1, "node_panic", 2, "bad_input", 3, "unknown_node", 4, "unsupported", 5,
        "internal", 6, "malformed_packet", 7);

    static final Map<String, Integer> LOG_LEVELS = Map.of(
        "error", 0, "warn", 1, "info", 2, "debug", 3, "trace", 4);

    public static final int RUN = 1, CANCEL = 2, STATE_EXPORT = 3, STATE_IMPORT = 4, PING = 5, SHUTDOWN = 6;
    public static final int HELLO = 0x40, OUTPUTS = 0x41, PROGRESS = 0x42, LOG = 0x43, FAULT = 0x44,
        STATE_VALUE = 0x45, PONG = 0x46;

    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private Wire() {}

    /** A type. Exactly one of the fields is meaningful, per {@code kind}. */
    public static final class Ty {
        public final String kind;
        public final String scalar;
        public final String key;
        public final List<Ty> items;
        public final List<String> names;
        public final List<Ty> payloads;

        Ty(String kind, String scalar, String key, List<Ty> items, List<String> names, List<Ty> payloads) {
            this.kind = kind;
            this.scalar = scalar;
            this.key = key;
            this.items = items;
            this.names = names;
            this.payloads = payloads;
        }

        static Ty scalar(String name) {
            return new Ty("scalar", name, null, List.of(), List.of(), List.of());
        }
    }

    /** An enum value: the variant's name and its payload, or null. */
    public record Variant(String name, Object payload) {}

    /** Parses a type written as text. Refuses anything it does not know. */
    public static Ty parseType(String text) {
        Ty ty = parse(text.trim(), 0);
        return ty;
    }

    private static Ty parse(String text, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        if (Arrays.asList(SCALARS).contains(text)) return Ty.scalar(text);
        if (text.startsWith("opaque(") && text.endsWith(")")) {
            return new Ty("opaque", null, text.substring(7, text.length() - 1), List.of(), List.of(), List.of());
        }
        if (text.startsWith("list<") && text.endsWith(">")) {
            return wrap("list", parse(text.substring(5, text.length() - 1), depth + 1));
        }
        if (text.startsWith("optional<") && text.endsWith(">")) {
            return wrap("optional", parse(text.substring(9, text.length() - 1), depth + 1));
        }
        if (text.startsWith("map<") && text.endsWith(">")) {
            List<String> parts = splitTop(text.substring(4, text.length() - 1));
            if (parts.size() != 2) throw new IllegalArgumentException("a map has two types: " + text);
            return new Ty("map", null, null, List.of(parse(parts.get(0), depth + 1), parse(parts.get(1), depth + 1)),
                List.of(), List.of());
        }
        if (text.startsWith("tuple<") && text.endsWith(">")) {
            List<Ty> items = new ArrayList<>();
            for (String part : splitTop(text.substring(6, text.length() - 1))) items.add(parse(part, depth + 1));
            return new Ty("tuple", null, null, items, List.of(), List.of());
        }
        if (text.startsWith("struct{") && text.endsWith("}")) {
            List<Ty> items = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (String part : splitTop(text.substring(7, text.length() - 1))) {
                int colon = part.indexOf(':');
                if (colon < 0) throw new IllegalArgumentException("a struct field is name:type: " + part);
                names.add(part.substring(0, colon).trim());
                items.add(parse(part.substring(colon + 1).trim(), depth + 1));
            }
            return new Ty("struct", null, null, items, names, List.of());
        }
        if (text.startsWith("enum{") && text.endsWith("}")) {
            List<Ty> payloads = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (String part : splitTop(text.substring(5, text.length() - 1))) {
                int colon = part.indexOf(':');
                if (colon < 0) {
                    names.add(part.trim());
                    payloads.add(null);
                } else {
                    names.add(part.substring(0, colon).trim());
                    payloads.add(parse(part.substring(colon + 1).trim(), depth + 1));
                }
            }
            return new Ty("enum", null, null, List.of(), names, payloads);
        }
        throw new IllegalArgumentException("unknown type: " + text);
    }

    private static Ty wrap(String kind, Ty inner) {
        return new Ty(kind, null, null, List.of(inner), List.of(), List.of());
    }

    /** Splits on commas that are not inside brackets. */
    static List<String> splitTop(String text) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '<' || c == '{' || c == '(') depth++;
            else if (c == '>' || c == '}' || c == ')') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(text.substring(start, i).trim());
                start = i + 1;
            }
        }
        if (start < text.length()) parts.add(text.substring(start).trim());
        return parts;
    }

    // ---- framing -----------------------------------------------------------------------------

    /** One packet's body, or null when the stream ended between packets. */
    public static byte[] readPacket(InputStream in, long cap) throws IOException {
        byte[] header = new byte[4];
        int got = readFully(in, header, true);
        if (got < 0) return null;
        long len = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
        if (len > cap) throw new IOException("packet of " + len + " bytes is over the cap of " + cap);
        byte[] body = new byte[(int) len];
        if (readFully(in, body, false) < 0) throw new IOException("the stream ended inside a packet");
        return body;
    }

    private static int readFully(InputStream in, byte[] buf, boolean allowEof) throws IOException {
        int got = 0;
        while (got < buf.length) {
            int n = in.read(buf, got, buf.length - got);
            if (n < 0) {
                if (allowEof && got == 0) return -1;
                throw new IOException("the stream ended inside a packet");
            }
            got += n;
        }
        return got;
    }

    public static void writePacket(OutputStream out, byte[] body, long cap) throws IOException {
        if (body.length > cap) throw new IOException("packet of " + body.length + " bytes is over the cap of " + cap);
        ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(body.length);
        out.write(header.array());
        out.write(body);
        out.flush();
    }

    // ---- bytes -------------------------------------------------------------------------------

    /** A writer of little-endian fields. */
    public static final class Writer {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        ByteBuffer buf(int size) {
            return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        }

        void u8(int v) { out.write(v & 0xFF); }
        void bool(boolean v) { u8(v ? 1 : 0); }
        void u16(int v) { out.writeBytes(buf(2).putShort((short) v).array()); }
        void u32(long v) { out.writeBytes(buf(4).putInt((int) v).array()); }
        void u64(BigInteger v) {
            if (v.signum() < 0 || v.compareTo(U64_MAX) > 0) throw new IllegalArgumentException(v + " is outside u64");
            out.writeBytes(buf(8).putLong(v.longValue()).array());
        }
        void i8(byte v) { out.write(v); }
        void i16(short v) { out.writeBytes(buf(2).putShort(v).array()); }
        void i32(int v) { out.writeBytes(buf(4).putInt(v).array()); }
        void i64(long v) { out.writeBytes(buf(8).putLong(v).array()); }
        void f32(float v) { out.writeBytes(buf(4).putFloat(v).array()); }
        void f64(double v) { out.writeBytes(buf(8).putDouble(v).array()); }
        void bytes(byte[] data) { u32(data.length); out.writeBytes(data); }
        void str(String text) { bytes(text.getBytes(StandardCharsets.UTF_8)); }
        byte[] toByteArray() { return out.toByteArray(); }
    }

    /** A reader of little-endian fields, refusing to read past its end. */
    public static final class Reader {
        private final ByteBuffer buf;

        public Reader(byte[] data) {
            this.buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        }

        int remaining() { return buf.remaining(); }

        byte[] take(int count) {
            if (count < 0 || count > buf.remaining()) {
                throw new IllegalArgumentException("input ends early: needs " + count + ", " + buf.remaining() + " left");
            }
            byte[] out = new byte[count];
            buf.get(out);
            return out;
        }

        int u8() { return take(1)[0] & 0xFF; }
        boolean bool() {
            int tag = u8();
            if (tag > 1) throw new IllegalArgumentException("bad bool " + tag);
            return tag == 1;
        }
        int u16() { return ByteBuffer.wrap(take(2)).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF; }
        long u32() { return ByteBuffer.wrap(take(4)).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL; }
        BigInteger u64() {
            long raw = ByteBuffer.wrap(take(8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
            return new BigInteger(Long.toUnsignedString(raw));
        }
        byte i8() { return take(1)[0]; }
        short i16() { return ByteBuffer.wrap(take(2)).order(ByteOrder.LITTLE_ENDIAN).getShort(); }
        int i32() { return ByteBuffer.wrap(take(4)).order(ByteOrder.LITTLE_ENDIAN).getInt(); }
        long i64() { return ByteBuffer.wrap(take(8)).order(ByteOrder.LITTLE_ENDIAN).getLong(); }
        float f32() { return ByteBuffer.wrap(take(4)).order(ByteOrder.LITTLE_ENDIAN).getFloat(); }
        double f64() { return ByteBuffer.wrap(take(8)).order(ByteOrder.LITTLE_ENDIAN).getDouble(); }

        /** A count, checked against the bytes left: each item takes at least one byte. */
        int count() {
            long count = u32();
            if (count > remaining()) throw new IllegalArgumentException("count " + count + " cannot fit");
            return (int) count;
        }

        byte[] bytes() { return take(count()); }

        String str() {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes())).toString();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException("a string field is not UTF-8", e);
            }
        }

        void finish() {
            if (remaining() != 0) throw new IllegalArgumentException(remaining() + " bytes after the message");
        }
    }

    // ---- types on the wire -------------------------------------------------------------------

    public static void encodeType(Ty ty, Writer out, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        switch (ty.kind) {
            case "scalar" -> { out.u8(0); out.u8(Arrays.asList(SCALARS).indexOf(ty.scalar)); }
            case "opaque" -> { out.u8(1); out.str(ty.key); }
            case "optional" -> { out.u8(2); encodeType(ty.items.get(0), out, depth + 1); }
            case "list" -> { out.u8(3); encodeType(ty.items.get(0), out, depth + 1); }
            case "map" -> { out.u8(4); encodeType(ty.items.get(0), out, depth + 1); encodeType(ty.items.get(1), out, depth + 1); }
            case "tuple" -> {
                out.u8(5); out.u32(ty.items.size());
                for (Ty item : ty.items) encodeType(item, out, depth + 1);
            }
            case "struct" -> {
                out.u8(6); out.u32(ty.items.size());
                for (int i = 0; i < ty.items.size(); i++) { out.str(ty.names.get(i)); encodeType(ty.items.get(i), out, depth + 1); }
            }
            case "enum" -> {
                out.u8(7); out.u32(ty.names.size());
                for (int i = 0; i < ty.names.size(); i++) {
                    out.str(ty.names.get(i));
                    Ty payload = ty.payloads.get(i);
                    if (payload == null) out.u8(0);
                    else { out.u8(1); encodeType(payload, out, depth + 1); }
                }
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        }
    }

    public static Ty decodeType(Reader in, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        int tag = in.u8();
        return switch (tag) {
            case 0 -> {
                int index = in.u8();
                if (index >= SCALARS.length) throw new IllegalArgumentException("bad scalar tag " + index);
                yield Ty.scalar(SCALARS[index]);
            }
            case 1 -> new Ty("opaque", null, in.str(), List.of(), List.of(), List.of());
            case 2 -> wrap("optional", decodeType(in, depth + 1));
            case 3 -> wrap("list", decodeType(in, depth + 1));
            case 4 -> new Ty("map", null, null, List.of(decodeType(in, depth + 1), decodeType(in, depth + 1)),
                List.of(), List.of());
            case 5 -> {
                int n = in.count();
                List<Ty> items = new ArrayList<>();
                for (int i = 0; i < n; i++) items.add(decodeType(in, depth + 1));
                yield new Ty("tuple", null, null, items, List.of(), List.of());
            }
            case 6 -> {
                int n = in.count();
                List<Ty> items = new ArrayList<>();
                List<String> names = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    names.add(in.str());
                    items.add(decodeType(in, depth + 1));
                }
                yield new Ty("struct", null, null, items, names, List.of());
            }
            case 7 -> {
                int n = in.count();
                List<String> names = new ArrayList<>();
                List<Ty> payloads = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    names.add(in.str());
                    payloads.add(in.u8() == 1 ? decodeType(in, depth + 1) : null);
                }
                yield new Ty("enum", null, null, List.of(), names, payloads);
            }
            default -> throw new IllegalArgumentException("bad type tag " + tag);
        };
    }

    // ---- values ------------------------------------------------------------------------------

    /** The ports and codecs one side knows. */
    public static final class Schema {
        private final Map<String, Ty> ports = new java.util.HashMap<>();
        private final Set<String> codecs;

        public Schema(Set<String> codecs) {
            this.codecs = codecs;
        }

        public void port(String node, String direction, String name, Ty ty) {
            ports.put(node + "\u0000" + direction + "\u0000" + name, ty);
        }

        Ty lookup(String node, String direction, String name) {
            Ty ty = ports.get(node + "\u0000" + direction + "\u0000" + name);
            if (ty == null) throw new IllegalArgumentException("node " + node + " has no " + direction + " " + name);
            return ty;
        }

        boolean hasCodec(String key) { return codecs.contains(key); }
    }

    public static void encodeValue(Ty ty, Object value, Schema schema, Writer out, int depth) {
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
                else { out.u8(1); encodeValue(ty.items.get(0), value, schema, out, depth + 1); }
            }
            case "list" -> {
                List<?> items = (List<?>) value;
                out.u32(items.size());
                for (Object item : items) encodeValue(ty.items.get(0), item, schema, out, depth + 1);
            }
            case "map" -> {
                Map<?, ?> map = (Map<?, ?>) value;
                out.u32(map.size());
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    encodeValue(ty.items.get(0), e.getKey(), schema, out, depth + 1);
                    encodeValue(ty.items.get(1), e.getValue(), schema, out, depth + 1);
                }
            }
            case "tuple" -> {
                Object[] items = (Object[]) value;
                if (items.length != ty.items.size()) throw new IllegalArgumentException("a tuple has the wrong length");
                for (int i = 0; i < items.length; i++) encodeValue(ty.items.get(i), items[i], schema, out, depth + 1);
            }
            case "struct" -> {
                Map<?, ?> fields = (Map<?, ?>) value;
                for (int i = 0; i < ty.items.size(); i++) {
                    String name = ty.names.get(i);
                    if (!fields.containsKey(name)) throw new IllegalArgumentException("struct has no field " + name);
                    encodeValue(ty.items.get(i), fields.get(name), schema, out, depth + 1);
                }
            }
            case "enum" -> {
                Variant variant = (Variant) value;
                int index = ty.names.indexOf(variant.name());
                if (index < 0) throw new IllegalArgumentException("no variant " + variant.name());
                out.u32(index);
                Ty payload = ty.payloads.get(index);
                if (payload != null) encodeValue(payload, variant.payload(), schema, out, depth + 1);
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        }
    }

    private static void encodeScalar(String name, Object value, Writer out) {
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
            case "u64", "usize" -> out.u64(unsigned(value));
            case "f32" -> out.f32(((Number) value).floatValue());
            case "f64" -> out.f64(((Number) value).doubleValue());
            case "string" -> out.str((String) value);
            case "bytes" -> out.bytes((byte[]) value);
            default -> throw new IllegalArgumentException("unknown scalar " + name);
        }
    }

    private static BigInteger range(Object value, long low, long high) {
        BigInteger v = BigInteger.valueOf(((Number) value).longValue());
        if (v.compareTo(BigInteger.valueOf(low)) < 0 || v.compareTo(BigInteger.valueOf(high)) > 0) {
            throw new IllegalArgumentException(v + " is outside the range");
        }
        return v;
    }

    private static BigInteger unsigned(Object value) {
        BigInteger v = value instanceof BigInteger big ? big : BigInteger.valueOf(((Number) value).longValue());
        if (v.signum() < 0 || v.compareTo(U64_MAX) > 0) throw new IllegalArgumentException(v + " is outside u64");
        return v;
    }

    public static Object decodeValue(Ty ty, Reader in, Schema schema, int depth) {
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
                yield decodeValue(ty.items.get(0), in, schema, depth + 1);
            }
            case "list" -> {
                int n = in.count();
                List<Object> items = new ArrayList<>(n);
                for (int i = 0; i < n; i++) items.add(decodeValue(ty.items.get(0), in, schema, depth + 1));
                yield items;
            }
            case "map" -> {
                int n = in.count();
                Map<Object, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < n; i++) {
                    Object key = decodeValue(ty.items.get(0), in, schema, depth + 1);
                    out.put(key, decodeValue(ty.items.get(1), in, schema, depth + 1));
                }
                yield out;
            }
            case "tuple" -> {
                Object[] items = new Object[ty.items.size()];
                for (int i = 0; i < items.length; i++) items[i] = decodeValue(ty.items.get(i), in, schema, depth + 1);
                yield items;
            }
            case "struct" -> {
                Map<String, Object> out = new LinkedHashMap<>();
                for (int i = 0; i < ty.items.size(); i++) {
                    out.put(ty.names.get(i), decodeValue(ty.items.get(i), in, schema, depth + 1));
                }
                yield out;
            }
            case "enum" -> {
                long index = in.u32();
                if (index >= ty.names.size()) throw new IllegalArgumentException("bad variant " + index);
                int i = (int) index;
                Ty payload = ty.payloads.get(i);
                yield new Variant(ty.names.get(i), payload == null ? null : decodeValue(payload, in, schema, depth + 1));
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        };
    }

    private static Object decodeScalar(String name, Reader in) {
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
            case "u64", "usize" -> in.u64();
            case "f32" -> in.f32();
            case "f64" -> in.f64();
            case "string" -> in.str();
            case "bytes" -> in.bytes();
            default -> throw new IllegalArgumentException("unknown scalar " + name);
        };
    }

    // ---- messages ----------------------------------------------------------------------------

    static String truncate(String text, int cap) {
        byte[] data = text.getBytes(StandardCharsets.UTF_8);
        if (data.length <= cap) return text;
        int cut = cap;
        while (cut > 0 && (data[cut] & 0xC0) == 0x80) cut--;
        return new String(data, 0, cut, StandardCharsets.UTF_8);
    }
}
