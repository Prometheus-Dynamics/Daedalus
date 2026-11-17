package dev.daedalus.wire;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Little-endian fields, length-prefixed strings and bytes, and records: the byte layout that both
 * the worker protocol and the service protocol are made of.
 *
 * <p>Integers come in two widths here. A {@code u32} is a {@code long} that is never negative, and
 * a {@code u64} is a {@code long} read as unsigned: the bits are the same as the wire's, so a value
 * above {@code Long.MAX_VALUE} is negative in Java and means the same number.
 *
 * <p>A reader refuses to read past the end of its input and throws {@link IllegalArgumentException}.
 * A record is a length followed by its fields. A reader takes the fields it knows and skips the rest
 * of the record, so a record can gain fields at its end.
 */
public final class Bytes {
    private Bytes() {}

    /** Builds one message body. */
    public static final class Writer {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        /** Writes one record: its length, then what {@code body} writes into it. */
        public void record(Body body) {
            Writer inner = new Writer();
            body.write(inner);
            byte[] fields = inner.toByteArray();
            u32(fields.length);
            out.writeBytes(fields);
        }

        /** What a record holds. */
        public interface Body {
            void write(Writer out);
        }

        public void u8(int v) { out.write(v & 0xFF); }
        public void bool(boolean v) { u8(v ? 1 : 0); }
        public void u16(int v) { out.writeBytes(le(2).putShort((short) v).array()); }
        public void u32(long v) {
            if (v < 0 || v > 0xFFFFFFFFL) throw new IllegalArgumentException(v + " is outside u32");
            out.writeBytes(le(4).putInt((int) v).array());
        }
        /** A u64 from its bits: a negative {@code long} is the unsigned value above {@code 2^63}. */
        public void u64(long v) { out.writeBytes(le(8).putLong(v).array()); }
        public void i8(byte v) { out.write(v); }
        public void i16(short v) { out.writeBytes(le(2).putShort(v).array()); }
        public void i32(int v) { out.writeBytes(le(4).putInt(v).array()); }
        public void i64(long v) { out.writeBytes(le(8).putLong(v).array()); }
        public void f32(float v) { out.writeBytes(le(4).putFloat(v).array()); }
        public void f64(double v) { out.writeBytes(le(8).putDouble(v).array()); }
        /** A count or length: a u32. */
        public void len(int count) { u32(count); }
        public void bytes(byte[] data) { u32(data.length); out.writeBytes(data); }
        public void str(String text) { bytes(text.getBytes(StandardCharsets.UTF_8)); }
        public byte[] toByteArray() { return out.toByteArray(); }

        private static ByteBuffer le(int size) {
            return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /** Reads one message body, front to back. */
    public static final class Reader {
        private final ByteBuffer buf;

        public Reader(byte[] data) {
            this.buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        }

        public int remaining() { return buf.remaining(); }

        /** Whether anything is left: a field added at the end of a record is read only then. */
        public boolean more() { return buf.hasRemaining(); }

        public byte[] take(int count) {
            if (count < 0 || count > buf.remaining()) {
                throw new IllegalArgumentException("input ends early: needs " + count + ", " + buf.remaining() + " left");
            }
            byte[] out = new byte[count];
            buf.get(out);
            return out;
        }

        public int u8() { return take(1)[0] & 0xFF; }

        public boolean bool() {
            int tag = u8();
            if (tag > 1) throw new IllegalArgumentException("bad bool " + tag);
            return tag == 1;
        }

        public int u16() { return le(2).getShort() & 0xFFFF; }
        public long u32() { return le(4).getInt() & 0xFFFFFFFFL; }
        /** A u64 as its bits: see {@link Writer#u64}. */
        public long u64() { return le(8).getLong(); }
        public byte i8() { return take(1)[0]; }
        public short i16() { return le(2).getShort(); }
        public int i32() { return le(4).getInt(); }
        public long i64() { return le(8).getLong(); }
        public float f32() { return le(4).getFloat(); }
        public double f64() { return le(8).getDouble(); }

        /** A count, checked against the bytes left: each item takes at least one byte. */
        public int count() {
            long count = u32();
            if (count > remaining()) throw new IllegalArgumentException("count " + count + " cannot fit");
            return (int) count;
        }

        public byte[] bytes() { return take(count()); }

        public String str() {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes())).toString();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException("a string field is not UTF-8", e);
            }
        }

        /** Reads a record: the fields {@code body} reads, and whatever else it holds is skipped. */
        public <T> T record(Function<Reader, T> body) {
            int len = count();
            return body.apply(new Reader(take(len)));
        }

        /** Refuses bytes left after a complete message. */
        public void finish() {
            if (remaining() != 0) throw new IllegalArgumentException(remaining() + " bytes after the message");
        }

        private ByteBuffer le(int size) {
            return ByteBuffer.wrap(take(size)).order(ByteOrder.LITTLE_ENDIAN);
        }
    }
}
