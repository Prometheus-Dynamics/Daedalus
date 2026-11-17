package dev.daedalus.worker;

/**
 * The worker layout hash, as a plain program: {@code java -cp <classes> dev.daedalus.worker.LayoutTest
 * <decimal hash>}. The argument is the Rust constant ({@code daedalus_wire::LAYOUT_HASH}), which the test that
 * runs this program computes on the Rust side. It exits non-zero when the literal in {@link Protocol} differs.
 */
public final class LayoutTest {
    private LayoutTest() {}

    public static void main(String[] args) {
        if (args.length != 1) throw new IllegalArgumentException("usage: LayoutTest <decimal Rust LAYOUT_HASH>");
        long rust = Long.parseUnsignedLong(args[0]);
        if (Protocol.LAYOUT_HASH != rust) {
            throw new AssertionError("the worker layout hash " + Long.toUnsignedString(Protocol.LAYOUT_HASH)
                + " is not the Rust constant " + Long.toUnsignedString(rust));
        }
        System.out.println("LayoutTest: the worker layout hash is the Rust constant");
    }
}
