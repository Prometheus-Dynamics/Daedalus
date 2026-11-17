package dev.daedalus.service;

import java.util.List;

/**
 * The service protocol's constants, enums and small records: what a client and {@code daedalusd}
 * say to each other. The rules for growing it are the ones in {@code core/wire/src/service.rs}:
 * a record gains fields only at its end and is read with its length; an op, event kind or error
 * code this side does not have is {@link OpKind#OTHER}, {@link Events.Unknown} or
 * {@link ErrorCode#OTHER}; a value that changes what a message means moves {@link #LAYOUT_HASH}.
 */
public final class Protocol {
    /** The service protocol version a hello names. There is no range. */
    public static final int VERSION = 1;
    /**
     * The hash of the service layout, from {@code daedalus_wire::service::SERVICE_LAYOUT_HASH}. A test
     * checks this literal against the Rust constant.
     */
    public static final long LAYOUT_HASH = 0xffa13d224a4e8519L;
    /** Most bytes a session name may have. */
    public static final int SESSION_CAP = 64;
    /** The session a request names when the client chose none. */
    public static final String DEFAULT_SESSION = "default";

    /** The kind bytes of the service's packets. They share no value with the worker protocol's. */
    public static final int CLIENT_HELLO = 0x80, REQUEST = 0x81, SERVER_HELLO = 0xc0, RESPONSE = 0xc1,
        EVENT = 0xc2;

    /** What this side knows of the things a peer may rely on. Empty, as in the Rust build. */
    public static final List<String> KNOWN = List.of();

    private Protocol() {}

    /** An enum with a number on the wire. */
    public interface Coded {
        int code();
    }

    /** Which op a request is. {@link #OTHER} is an op this side does not have, named in a list. */
    public enum OpKind implements Coded {
        LOAD_PLUGIN(1, "load_plugin"), PUT_GRAPH(2, "put_graph"), PATCH_GRAPH(3, "patch_graph"),
        PLAN(4, "plan"), START(5, "start"), FEED(6, "feed"), STEP(7, "step"), TAKE(8, "take"),
        WATCH(9, "watch"), UNWATCH(10, "unwatch"), RELOAD(11, "reload"), CANCEL(12, "cancel"),
        STOP(13, "stop"), INSPECT(14, "inspect"), CACHE(15, "cache"), TRACE(16, "trace"), PING(17, "ping"),
        MAKE_PLAN(18, "make_plan"), PUT_PLAN(19, "put_plan"), OTHER(255, "other");

        private final int code;
        private final String label;

        OpKind(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        /** The name a log or a command line shows. */
        public String label() { return label; }

        /** The op a list names: one this side doesn't have is {@link #OTHER}. */
        public static OpKind named(int code) {
            for (OpKind kind : values()) if (kind.code == code && kind != OTHER) return kind;
            return OTHER;
        }

        /** Whether sending the op twice does what sending it once does. */
        public boolean idempotent() {
            return this == PING || this == INSPECT || this == TRACE;
        }
    }

    /** A refused request's code. {@link #OTHER} is a code this side does not have. */
    public enum ErrorCode implements Coded {
        INVALID_REQUEST(1, "invalid_request"), UNKNOWN_GRAPH(2, "unknown_graph"), UNKNOWN_RUN(3, "unknown_run"),
        UNKNOWN_PORT(4, "unknown_port"), VALUE_TYPE(5, "value_type"), PLUGIN_ERROR(6, "plugin_error"),
        PLANNING_FAILED(7, "planning_failed"), BUSY(8, "busy"), CANCELLED(9, "cancelled"),
        STOPPED(10, "stopped"), FRAME_TOO_LARGE(11, "frame_too_large"), UNSUPPORTED(12, "unsupported"),
        OTHER(65535, "other");

        private final int code;
        private final String label;

        ErrorCode(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        /** The name a log or a command line shows. */
        public String label() { return label; }

        /** The code a number names. One this side doesn't have is {@link #OTHER}. */
        public static ErrorCode fromCode(int code) {
            for (ErrorCode error : values()) if (error.code == code && error != OTHER) return error;
            return OTHER;
        }
    }

    /** How serious a planner diagnostic is. */
    public enum Severity implements Coded {
        ERROR(0, "error"), WARNING(1, "warning");

        private final int code;
        private final String label;

        Severity(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** The form a plan is asked for in. */
    public enum PlanForm implements Coded {
        STEPS(0, "steps"), BLOB(1, "blob"), SOURCE(2, "source");

        private final int code;
        private final String label;

        PlanForm(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** How much a run records. */
    public enum TelemetryLevel implements Coded {
        OFF(0, "off"), BASIC(1, "basic"), DETAILED(2, "detailed"), TRACE(3, "trace");

        private final int code;
        private final String label;

        TelemetryLevel(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** How a run stops. */
    public enum StopMode implements Coded {
        DRAIN(0, "drain"), IMMEDIATE(1, "immediate");

        private final int code;
        private final String label;

        StopMode(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** Where a run is in its life. */
    public enum RunState implements Coded {
        RUNNING(0, "running"), CANCELLED(1, "cancelled"), STOPPED(2, "stopped");

        private final int code;
        private final String label;

        RunState(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** Why a watch, or a run it follows, ended. */
    public enum EndReason implements Coded {
        STOPPED(0, "stopped"), CANCELLED(1, "cancelled"), UNWATCHED(2, "unwatched"), SHUTDOWN(3, "shutdown");

        private final int code;
        private final String label;

        EndReason(int code, String label) {
            this.code = code;
            this.label = label;
        }

        @Override public int code() { return code; }

        public String label() { return label; }
    }

    /** What a server offers: its ops, the value codecs it has, and its run limit (zero is none). */
    public record Capabilities(List<OpKind> ops, List<String> codecs, long maxRuns) {}

    /**
     * The first packet each way. A client sends one with empty capabilities, and the server answers
     * with its own. {@code packetCap} is the largest packet this side reads.
     */
    public record ServiceHello(long version, long layout, long packetCap, Capabilities capabilities,
                               List<String> knows, List<String> needs) {
        /** A client's hello: no capabilities, nothing known beyond {@link #KNOWN}. */
        public static ServiceHello client(long packetCap) {
            return new ServiceHello(VERSION, LAYOUT_HASH, packetCap,
                new Capabilities(List.of(), List.of(), 0), KNOWN, List.of());
        }

        /**
         * Why a peer with this hello is refused, or null when it is accepted: another version or
         * layout, or a need this side does not know.
         */
        public String refusal() {
            if (version != VERSION) return "the server speaks service version " + version + ", not " + VERSION;
            if (layout != LAYOUT_HASH) {
                return "the server's layout is " + Long.toUnsignedString(layout) + ", not "
                    + Long.toUnsignedString(LAYOUT_HASH);
            }
            for (String name : needs) {
                if (!KNOWN.contains(name)) {
                    return "the other side needs `" + name + "`, which this side doesn't have: this side has to be updated";
                }
            }
            return null;
        }
    }

    /** One thing the planner or a lint found in a graph. */
    public record Diagnostic(Severity severity, String code, String message, String node, String port) {}

    /** A refused request: its code, its message, and what the planner found when planning failed. */
    public record ServiceError(ErrorCode code, String message, List<Diagnostic> diagnostics)
        implements Outcome {
        public ServiceError(ErrorCode code, String message) {
            this(code, message, List.of());
        }
    }

    /** The answer to one request: a reply, or a refusal. */
    public sealed interface Outcome permits Reply, ServiceError {}
}
