package dev.daedalus.wire;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Packets: a u32 length, little-endian, then the body. Both protocols put a length before each body. */
public final class Packets {
    /** The largest packet a side reads by default: 16 MiB. */
    public static final long DEFAULT_CAP = 16L * 1024 * 1024;

    private Packets() {}

    /** One packet's body, or null when the stream ended between packets. */
    public static byte[] read(InputStream in, long cap) throws IOException {
        byte[] header = new byte[4];
        int got = fill(in, header, true);
        if (got < 0) return null;
        long len = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
        if (len > cap) throw new IOException("packet of " + len + " bytes is over the cap of " + cap);
        byte[] body = new byte[(int) len];
        if (fill(in, body, false) < 0) throw new IOException("the stream ended inside a packet");
        return body;
    }

    /** Writes one packet and flushes. */
    public static void write(OutputStream out, byte[] body, long cap) throws IOException {
        if (body.length > cap) throw new IOException("packet of " + body.length + " bytes is over the cap of " + cap);
        ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        header.putInt(body.length);
        out.write(header.array());
        out.write(body);
        out.flush();
    }

    private static int fill(InputStream in, byte[] buf, boolean allowEof) throws IOException {
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
}
