package dev.daedalus.wire;

import dev.daedalus.wire.Types.Ty;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The value codec's checks, as a plain program: {@code java -cp <classes> dev.daedalus.wire.WireTest}.
 * It exits non-zero on the first failed check.
 */
public final class WireTest {
    private WireTest() {}

    static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError("failed: " + what);
    }

    static Object roundTrip(Ty ty, Object value) {
        Values.Schema schema = new Values.Schema(Set.of("fixture.custom"));
        Bytes.Writer out = new Bytes.Writer();
        Values.encode(ty, value, schema, out, 0);
        Bytes.Reader in = new Bytes.Reader(out.toByteArray());
        Object back = Values.decode(ty, in, schema, 0);
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
        check(roundTrip(Types.parse("u64"), new BigInteger("18446744073709551615"))
            .equals(new BigInteger("18446744073709551615")), "u64 max round trips");
        check(refused(() -> roundTrip(Types.parse("u64"), -1L)), "a negative u64 is refused");
        check(refused(() -> roundTrip(Types.parse("u32"), 4294967296L)), "u32 past its range is refused");
        check(refused(() -> roundTrip(Types.parse("u8"), 256)), "u8 past its range is refused");
        byte[] bytes = {0, 1, (byte) 255};
        check(Arrays.equals((byte[]) roundTrip(Types.parse("bytes"), bytes), bytes), "bytes round trip");
        check(Arrays.equals((byte[]) roundTrip(Types.parse("bytes"), new byte[0]), new byte[0]), "empty bytes round trip");
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("name", "demo");
        record.put("count", 3L);
        Ty struct = Types.parse("struct{name:string,count:i64}");
        check(roundTrip(struct, record).equals(record), "a struct round trips");
        check(refused(() -> roundTrip(Types.parse("opaque(other.key)"), new byte[] {1})),
            "an opaque value needs a codec");
        check(Types.splitTop("a<b,c>,d").size() == 2, "splitTop keeps brackets together");
        check(refused(() -> Types.parse("list<u32")), "an unclosed type is refused");
        System.out.println("WireTest: all checks pass");
    }
}
