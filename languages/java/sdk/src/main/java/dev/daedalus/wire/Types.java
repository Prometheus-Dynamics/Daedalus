package dev.daedalus.wire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Types, as the wire carries them, and as the SDKs write them in text.
 *
 * <p>A type in text is {@code i64}, {@code string}, {@code bytes}, {@code list<T>}, {@code optional<T>},
 * {@code map<K,V>}, {@code tuple<A,B>}, {@code struct{name:T,count:T}}, {@code enum{idle,busy:T}}, or
 * {@code opaque(key)}. {@link #parse} reads it and refuses anything else.
 */
public final class Types {
    public static final int MAX_DEPTH = 32;

    public static final String[] SCALARS = {
        "unit", "bool", "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize",
        "f32", "f64", "string", "bytes",
    };

    private Types() {}

    /** A type. Exactly the fields its {@code kind} uses are meaningful. */
    public static final class Ty {
        public final String kind;
        public final String scalar;
        public final String key;
        public final List<Ty> items;
        public final List<String> names;
        /** Enum payloads: one per name, null where a variant has none. */
        public final List<Ty> payloads;

        Ty(String kind, String scalar, String key, List<Ty> items, List<String> names, List<Ty> payloads) {
            this.kind = kind;
            this.scalar = scalar;
            this.key = key;
            this.items = items;
            this.names = names;
            this.payloads = payloads;
        }

        /** A scalar type by its name, as in {@link #SCALARS}. */
        public static Ty scalar(String name) {
            return new Ty("scalar", name, null, List.of(), List.of(), List.of());
        }

        public static Ty opaque(String key) {
            return new Ty("opaque", null, key, List.of(), List.of(), List.of());
        }

        public static Ty wrap(String kind, Ty inner) {
            return new Ty(kind, null, null, List.of(inner), List.of(), List.of());
        }

        public static Ty map(Ty key, Ty value) {
            return new Ty("map", null, null, List.of(key, value), List.of(), List.of());
        }

        public static Ty tuple(List<Ty> items) {
            return new Ty("tuple", null, null, items, List.of(), List.of());
        }

        public static Ty struct(List<String> names, List<Ty> items) {
            return new Ty("struct", null, null, items, names, List.of());
        }

        public static Ty enumeration(List<String> names, List<Ty> payloads) {
            return new Ty("enum", null, null, List.of(), names, payloads);
        }
    }

    /** Parses a type written as text. Refuses anything it does not know. */
    public static Ty parse(String text) {
        return parse(text.trim(), 0);
    }

    private static Ty parse(String text, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        if (Arrays.asList(SCALARS).contains(text)) return Ty.scalar(text);
        if (text.startsWith("opaque(") && text.endsWith(")")) {
            return Ty.opaque(text.substring(7, text.length() - 1));
        }
        if (text.startsWith("list<") && text.endsWith(">")) {
            return Ty.wrap("list", parse(text.substring(5, text.length() - 1), depth + 1));
        }
        if (text.startsWith("optional<") && text.endsWith(">")) {
            return Ty.wrap("optional", parse(text.substring(9, text.length() - 1), depth + 1));
        }
        if (text.startsWith("map<") && text.endsWith(">")) {
            List<String> parts = splitTop(text.substring(4, text.length() - 1));
            if (parts.size() != 2) throw new IllegalArgumentException("a map has two types: " + text);
            return Ty.map(parse(parts.get(0), depth + 1), parse(parts.get(1), depth + 1));
        }
        if (text.startsWith("tuple<") && text.endsWith(">")) {
            List<Ty> items = new ArrayList<>();
            for (String part : splitTop(text.substring(6, text.length() - 1))) items.add(parse(part, depth + 1));
            return Ty.tuple(items);
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
            return Ty.struct(names, items);
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
            return Ty.enumeration(names, payloads);
        }
        throw new IllegalArgumentException("unknown type: " + text);
    }

    /** Splits on commas that are not inside brackets. */
    public static List<String> splitTop(String text) {
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

    /** Writes a type's tree: the tags are the ones the Rust codec writes. */
    public static void encode(Ty ty, Bytes.Writer out, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        switch (ty.kind) {
            case "scalar" -> { out.u8(0); out.u8(Arrays.asList(SCALARS).indexOf(ty.scalar)); }
            case "opaque" -> { out.u8(1); out.str(ty.key); }
            case "optional" -> { out.u8(2); encode(ty.items.get(0), out, depth + 1); }
            case "list" -> { out.u8(3); encode(ty.items.get(0), out, depth + 1); }
            case "map" -> {
                out.u8(4);
                encode(ty.items.get(0), out, depth + 1);
                encode(ty.items.get(1), out, depth + 1);
            }
            case "tuple" -> {
                out.u8(5);
                out.len(ty.items.size());
                for (Ty item : ty.items) encode(item, out, depth + 1);
            }
            case "struct" -> {
                out.u8(6);
                out.len(ty.items.size());
                for (int i = 0; i < ty.items.size(); i++) {
                    out.str(ty.names.get(i));
                    encode(ty.items.get(i), out, depth + 1);
                }
            }
            case "enum" -> {
                out.u8(7);
                out.len(ty.names.size());
                for (int i = 0; i < ty.names.size(); i++) {
                    out.str(ty.names.get(i));
                    Ty payload = ty.payloads.get(i);
                    if (payload == null) out.u8(0);
                    else {
                        out.u8(1);
                        encode(payload, out, depth + 1);
                    }
                }
            }
            default -> throw new IllegalArgumentException("unknown type " + ty.kind);
        }
    }

    /** Reads a type's tree. Refuses a tag it does not know. */
    public static Ty decode(Bytes.Reader in, int depth) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("type nests too deep");
        int tag = in.u8();
        return switch (tag) {
            case 0 -> {
                int index = in.u8();
                if (index >= SCALARS.length) throw new IllegalArgumentException("bad scalar tag " + index);
                yield Ty.scalar(SCALARS[index]);
            }
            case 1 -> Ty.opaque(in.str());
            case 2 -> Ty.wrap("optional", decode(in, depth + 1));
            case 3 -> Ty.wrap("list", decode(in, depth + 1));
            case 4 -> Ty.map(decode(in, depth + 1), decode(in, depth + 1));
            case 5 -> {
                int n = in.count();
                List<Ty> items = new ArrayList<>();
                for (int i = 0; i < n; i++) items.add(decode(in, depth + 1));
                yield Ty.tuple(items);
            }
            case 6 -> {
                int n = in.count();
                List<String> names = new ArrayList<>();
                List<Ty> items = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    names.add(in.str());
                    items.add(decode(in, depth + 1));
                }
                yield Ty.struct(names, items);
            }
            case 7 -> {
                int n = in.count();
                List<String> names = new ArrayList<>();
                List<Ty> payloads = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    names.add(in.str());
                    int flag = in.u8();
                    if (flag > 1) throw new IllegalArgumentException("bad payload flag " + flag);
                    payloads.add(flag == 1 ? decode(in, depth + 1) : null);
                }
                yield Ty.enumeration(names, payloads);
            }
            default -> throw new IllegalArgumentException("bad type tag " + tag);
        };
    }
}
