package dev.daedalus.worker;

import java.util.Map;

/** The worker protocol's numbers: message kinds, fault codes, log levels and the layout hash. */
final class Protocol {
    /** The protocol version a hello names. */
    static final int PROTOCOL_VERSION = 1;
    /** Most bytes of a fault's reason that the host keeps. Longer text is cut at a character. */
    static final int REASON_CAP = 512;
    /**
     * FNV-1a over the words of the Rust layout (core/wire, {@code LAYOUT_HASH}). A hello carries it.
     * A test checks this literal against the Rust constant (see {@code WireTest}).
     */
    static final long LAYOUT_HASH = 0xebbd19e914919865L;

    static final int RUN = 1, CANCEL = 2, STATE_EXPORT = 3, STATE_IMPORT = 4, PING = 5, SHUTDOWN = 6;
    static final int HELLO = 0x40, OUTPUTS = 0x41, PROGRESS = 0x42, LOG = 0x43, FAULT = 0x44,
        STATE_VALUE = 0x45, PONG = 0x46;
    /** Added after the layout was fixed, and not part of its hash: a host ignores a kind it doesn't know. */
    static final int DETAIL = 0x47;
    /** The node's own code for a failure. Added as {@link #DETAIL} was. */
    static final int NODE_CODE = 0x48;

    /** How a run ended, as the outputs packet names it. */
    static final int OK = 0, CANCELLED = 1, FAULT_STATUS = 2;

    static final Map<String, Integer> FAULT_CODES = Map.of(
        "node_error", 1, "node_panic", 2, "bad_input", 3, "unknown_node", 4, "unsupported", 5,
        "internal", 6, "malformed_packet", 7);

    static final Map<String, Integer> LOG_LEVELS = Map.of(
        "error", 0, "warn", 1, "info", 2, "debug", 3, "trace", 4);

    private Protocol() {}
}
