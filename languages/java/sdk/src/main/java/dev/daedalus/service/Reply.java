package dev.daedalus.service;

import dev.daedalus.service.Model.CarryReport;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Model.PluginInfo;
import dev.daedalus.service.Model.PlanSummary;
import dev.daedalus.service.Model.StepSummary;
import dev.daedalus.service.Model.TraceEntry;
import dev.daedalus.service.Protocol.Diagnostic;
import java.util.List;

/** A successful answer. One variant per op's reply. */
public sealed interface Reply extends Protocol.Outcome {
    /** The protocol version the server speaks. */
    record Pong(long version) implements Reply {}

    /** Done, with nothing to say: unwatch, cancel, cache clear, put plan. */
    record Ack() implements Reply {}

    record Plugin(PluginInfo info) implements Reply {}

    /** A graph was stored or patched. The diagnostics are what the lints found in it. */
    record Graph(String name, List<Diagnostic> diagnostics) implements Reply {}

    record Plan(PlanSummary summary) implements Reply {}

    record Started(long run) implements Reply {}

    /** A feed was taken. {@code dropped} is true when the port's policy gave a value up for it. */
    record Fed(long woke, boolean dropped) implements Reply {}

    record Stepped(StepSummary summary) implements Reply {}

    /** What a take found. {@code value} is null when the port had nothing. */
    record Value(TypedValue value) implements Reply {}

    record Watching(long sub) implements Reply {}

    record Reloaded(CarryReport report) implements Reply {}

    /** A run stopped. {@code dropped} counts the values an immediate stop threw away. */
    record Stopped(long run, long tick, long dropped) implements Reply {}

    record Inspected(Inspection inspection) implements Reply {}

    record Trace(List<TraceEntry> entries) implements Reply {}

    /** A made plan: its bytes, and a hash of them that names the plan. */
    record Made(long hash, byte[] bytes) implements Reply {}
}
