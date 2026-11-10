package dev.daedalus.worker;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The codec's checks, as a plain program: run with {@code java -cp <classes> dev.daedalus.worker.WireTest}.
 * It exits non-zero on the first failed check.
 */
public final class WireTest {
    private WireTest() {}

    static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError("failed: " + what);
    }

    static Object roundTrip(Wire.Ty ty, Object value) {
        Wire.Schema schema = new Wire.Schema(Set.of("fixture.custom"));
        Wire.Writer out = new Wire.Writer();
        Wire.encodeValue(ty, value, schema, out, 0);
        Wire.Reader in = new Wire.Reader(out.toByteArray());
        Object back = Wire.decodeValue(ty, in, schema, 0);
        in.finish();
        return back;
    }

    static boolean refused(Runnable action) {
        try {
            action.run();
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    public static void main(String[] args) {
        check(roundTrip(Wire.parseType("u64"), new BigInteger("18446744073709551615"))
            .equals(new BigInteger("18446744073709551615")), "u64 max round trips");
        check(refused(() -> roundTrip(Wire.parseType("u64"), -1L)), "a negative u64 is refused");
        check(refused(() -> roundTrip(Wire.parseType("u32"), 4294967296L)), "u32 past its range is refused");
        check(refused(() -> roundTrip(Wire.parseType("u8"), 256)), "u8 past its range is refused");
        byte[] bytes = {0, 1, (byte) 255};
        check(java.util.Arrays.equals((byte[]) roundTrip(Wire.parseType("bytes"), bytes), bytes), "bytes round trip");
        check(java.util.Arrays.equals((byte[]) roundTrip(Wire.parseType("bytes"), new byte[0]), new byte[0]),
            "empty bytes round trip");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("name", "demo");
        record.put("count", 3L);
        Wire.Ty struct = Wire.parseType("struct{name:string,count:i64}");
        check(roundTrip(struct, record).equals(record), "a struct round trips");
        check(refused(() -> roundTrip(Wire.parseType("opaque(other.key)"), new byte[] {1})),
            "an opaque value needs a codec");
        check(Wire.LAYOUT_HASH == 0xebbd19e914919865L, "the layout hash is the Rust constant");
        check(Wire.splitTop("a<b,c>,d").size() == 2, "splitTop keeps brackets together");
        System.out.println("WireTest: all checks pass");
    }
}
