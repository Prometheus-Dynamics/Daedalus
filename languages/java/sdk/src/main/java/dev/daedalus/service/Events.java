package dev.daedalus.service;

import dev.daedalus.service.Protocol.EndReason;
import java.util.List;

/** What a watch delivers. */
public final class Events {
    private Events() {}

    /** One event of one watch. {@code sub} is the server's number for the subscription. */
    public record Event(long sub, EventBody body) {}

    /** The events a watch can deliver. */
    public sealed interface EventBody permits Events.Fault, Events.Telemetry, Events.Lagged, Events.Ended,
        Events.Unknown {}

    /**
     * A step that failed. {@code node} is null when the step gave none. {@code code} is the node's own
     * code, unless {@code ownCode}; zero when the node gave none.
     */
    public record Fault(long run, long tick, long step, String node, List<String> covers, long consecutive,
                        String reason, boolean reasonCut, String detail, boolean detailCut, long code,
                        boolean ownCode) implements EventBody {}

    /** What one batch of ticks did. */
    public record Telemetry(long run, long tick, long stepCalls, long faults, long skipped) implements EventBody {}

    /** The watcher was too slow: this many events were dropped, oldest first. */
    public record Lagged(long dropped) implements EventBody {}

    /** A run this watch follows ended, or the watch itself did. {@code run} is zero for a watch with none. */
    public record Ended(long run, EndReason reason) implements EventBody {}

    /** An event of a kind this side doesn't have. Nothing is taken to be something else. */
    public record Unknown(int kind) implements EventBody {}
}
