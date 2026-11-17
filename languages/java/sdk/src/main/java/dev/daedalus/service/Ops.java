package dev.daedalus.service;

import dev.daedalus.service.Protocol.OpKind;
import dev.daedalus.service.Protocol.PlanForm;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.Protocol.TelemetryLevel;
import java.util.List;

/** What a client asks for. An op is one kind byte, then its fields in a fixed order. */
public final class Ops {
    private Ops() {}

    /** One request's op. */
    public sealed interface Op permits Ops.LoadPlugin, Ops.PutGraph, Ops.PatchGraph, Ops.Plan, Ops.Start,
        Ops.Feed, Ops.Step, Ops.Take, Ops.Watch, Ops.Unwatch, Ops.Reload, Ops.Cancel, Ops.Stop, Ops.Inspect,
        Ops.CacheClear, Ops.Trace, Ops.Ping, Ops.MakePlan, Ops.PutPlan {
        OpKind kind();
    }

    /** How to plan a graph. {@code priority} is backend ids in the order ties go to; empty keeps the host's. */
    public record PlanOptions(PlanForm form, boolean inspect, List<String> priority) {
        public static PlanOptions defaults() {
            return new PlanOptions(PlanForm.STEPS, false, List.of());
        }
    }

    /**
     * How to start a run. A zero leaves the host's default; {@code telemetry} is what a run records.
     */
    public record StartConfig(long fifoCapacity, long faultReports, TelemetryLevel telemetry, boolean unmerged,
                              List<String> priority) {
        public static StartConfig defaults() {
            return new StartConfig(0, 0, TelemetryLevel.BASIC, false, List.of());
        }
    }

    /** The event kinds a watch asks for. {@code lagged} and {@code ended} always arrive. */
    public record EventKinds(int bits) {
        public static final EventKinds FAULT = new EventKinds(1);
        public static final EventKinds TELEMETRY = new EventKinds(2);
        public static final EventKinds ALL = new EventKinds(3);
    }

    /** What a watch follows. */
    public sealed interface WatchTarget permits WatchTarget.Everything, WatchTarget.OneRun {
        /** Every run, including ones started later. */
        record Everything() implements WatchTarget {}

        /** One run. */
        record OneRun(long run) implements WatchTarget {}
    }

    /** What an inspect looks at. */
    public sealed interface InspectTarget permits InspectTarget.OfRun, InspectTarget.OfGraph,
        InspectTarget.OfCache, InspectTarget.OfTelemetry, InspectTarget.OfHost {
        record OfRun(long run) implements InspectTarget {}

        /** A stored graph, by name, in the request's session. */
        record OfGraph(String name) implements InspectTarget {}

        record OfCache() implements InspectTarget {}

        record OfTelemetry(long run) implements InspectTarget {}

        record OfHost() implements InspectTarget {}
    }

    public record LoadPlugin(String path) implements Op {
        public OpKind kind() { return OpKind.LOAD_PLUGIN; }
    }

    /** Stores a graph document: JSON text in the graph format. */
    public record PutGraph(String name, String json) implements Op {
        public OpKind kind() { return OpKind.PUT_GRAPH; }
    }

    /** Applies a patch document (JSON text) to a stored graph. */
    public record PatchGraph(String graph, String patch) implements Op {
        public OpKind kind() { return OpKind.PATCH_GRAPH; }
    }

    public record Plan(String graph, PlanOptions options) implements Op {
        public OpKind kind() { return OpKind.PLAN; }
    }

    public record Start(String graph, StartConfig config) implements Op {
        public OpKind kind() { return OpKind.START; }
    }

    public record Feed(long run, String port, TypedValue value) implements Op {
        public OpKind kind() { return OpKind.FEED; }
    }

    public record Step(long run, long count) implements Op {
        public OpKind kind() { return OpKind.STEP; }
    }

    public record Take(long run, String port) implements Op {
        public OpKind kind() { return OpKind.TAKE; }
    }

    public record Watch(WatchTarget target, EventKinds kinds) implements Op {
        public OpKind kind() { return OpKind.WATCH; }
    }

    public record Unwatch(long sub) implements Op {
        public OpKind kind() { return OpKind.UNWATCH; }
    }

    public record Reload(long run, String graph) implements Op {
        public OpKind kind() { return OpKind.RELOAD; }
    }

    public record Cancel(long run) implements Op {
        public OpKind kind() { return OpKind.CANCEL; }
    }

    public record Stop(long run, StopMode mode) implements Op {
        public OpKind kind() { return OpKind.STOP; }
    }

    public record Inspect(InspectTarget target) implements Op {
        public OpKind kind() { return OpKind.INSPECT; }
    }

    /** Clears the plan cache. The only cache action. */
    public record CacheClear() implements Op {
        public OpKind kind() { return OpKind.CACHE; }
    }

    /** The newest {@code limit} served requests. Zero is all the server keeps. */
    public record Trace(long limit) implements Op {
        public OpKind kind() { return OpKind.TRACE; }
    }

    public record Ping() implements Op {
        public OpKind kind() { return OpKind.PING; }
    }

    /** Plans a stored graph with the options a start would use, and answers with the made plan. */
    public record MakePlan(String graph, StartConfig options) implements Op {
        public OpKind kind() { return OpKind.MAKE_PLAN; }
    }

    /** Stores a made plan under a name. Its bytes are checked when a run binds to it. */
    public record PutPlan(String name, byte[] bytes) implements Op {
        public OpKind kind() { return OpKind.PUT_PLAN; }
    }
}
