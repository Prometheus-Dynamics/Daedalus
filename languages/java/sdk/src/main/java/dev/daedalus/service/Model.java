package dev.daedalus.service;

import dev.daedalus.service.Protocol.ErrorCode;
import dev.daedalus.service.Protocol.OpKind;
import dev.daedalus.service.Protocol.PlanForm;
import dev.daedalus.service.Protocol.RunState;
import dev.daedalus.service.Protocol.TelemetryLevel;
import java.util.List;

/** The plain data the replies carry. Counts are {@code long}: a u32 is never negative. */
public final class Model {
    private Model() {}

    /** What a loaded plugin brought. */
    public record PluginInfo(String name, long generation, List<String> nodes, List<String> backends) {}

    /** What planning a graph gave. {@code explain} is null unless the request asked to inspect. */
    public record PlanSummary(String graph, long hash, PlanForm form, long nodes, long edges, long regions,
                              long steps, boolean cached, List<Protocol.Diagnostic> diagnostics, String explain) {}

    /** What a batch of ticks did. */
    public record StepSummary(long ticks, long tick, long stepCalls, long faults, long skipped, boolean idle) {}

    /** A list of names cut at a bound, with the exact count. */
    public record Names(List<String> names, long total) {}

    /** The state carry report of a reload. */
    public record CarryReport(Names carried, Names fresh, Names added, Names removed, Names removedPorts,
                              long droppedValues) {}

    /** One served request, as the trace keeps it. {@code error} is null when the request succeeded. */
    public record TraceEntry(long seq, long id, String session, OpKind op, String target, ErrorCode error) {}

    /** One run in a host listing. */
    public record RunBrief(long run, String session, String graph, RunState state) {}

    /** What an inspect found. */
    public sealed interface Inspection {
        /** A port of a run. */
        record Port(String name, boolean input, String typeKey) {}

        /** One run. */
        record Run(long run, String session, String graph, RunState state, long tick, long faults,
                   List<Port> ports, List<String> nodes) implements Inspection {}

        /** A stored graph's JSON. */
        record Graph(String name, String json) implements Inspection {}

        /** The plan cache's counters. */
        record Cache(long entries, long hits, long misses) implements Inspection {}

        /** One step's record in a run's telemetry. */
        record Row(long step, List<String> nodes, long calls, long failures, boolean backingOff, boolean disabled,
                   boolean autoOff, long timedRuns, long timeNs, long bytes) {}

        /** A run's telemetry. */
        record Telemetry(long run, TelemetryLevel level, long tick, long stepCalls, long faults, long skipped,
                         List<Row> rows) implements Inspection {}

        /** The host as a whole. */
        record Host(List<String> sessions, List<String> graphs, List<RunBrief> runs, List<String> plugins)
            implements Inspection {}
    }
}
