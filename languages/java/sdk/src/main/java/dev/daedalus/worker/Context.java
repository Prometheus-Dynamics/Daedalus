package dev.daedalus.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What a node body can see and report. Events go out, in order, before the run's outputs.
 *
 * <p>A body that loops should poll {@link #cancelled()}: the host cancels a run by setting that flag,
 * and a body that returns while it is set ends the run as cancelled. The flag is read from a volatile
 * field, so a poll sees a cancel without a delay.
 */
public final class Context {
    /** One event a body reported. */
    public record Event(String kind, float fraction, String message, String level) {}

    private final AtomicBoolean cancel;
    private Object state;
    private boolean stateSet;
    private Map<String, Object> inputs = Map.of();
    private final List<Event> events = new ArrayList<>();

    Context(Object state, AtomicBoolean cancel) {
        this.state = state;
        this.cancel = cancel;
    }

    /** Whether the host has cancelled this run. Poll it in a loop, and return once it is true. */
    public boolean cancelled() {
        return cancel.get();
    }

    /** The node's state from its last run, or null. */
    public Object state() {
        return state;
    }

    public void setState(Object value) {
        this.state = value;
        this.stateSet = true;
    }

    /** An integer input (i64, u32 or any integer that fits a long). */
    public long inputLong(String port) {
        return ((Number) input(port)).longValue();
    }

    /** A boolean input. */
    public boolean inputBool(String port) {
        return (Boolean) input(port);
    }

    /** A string input. */
    public String inputString(String port) {
        return (String) input(port);
    }

    /** A bytes input. */
    public byte[] inputBytes(String port) {
        return (byte[]) input(port);
    }

    private Object input(String port) {
        Object value = inputs.get(port);
        if (value == null) throw new IllegalArgumentException("no input " + port);
        return value;
    }

    public void progress(float fraction, String message) {
        events.add(new Event("progress", fraction, message, null));
    }

    public void log(String level, String text) {
        events.add(new Event("log", 0f, text, level));
    }

    /**
     * Gives the host more about a failure than the fault's reason holds. Call it any number of
     * times before throwing, and the pieces are kept in order with the step's fault. What a node
     * writes and then doesn't fail is dropped by the host.
     */
    public void detail(String text) {
        events.add(new Event("detail", 0f, text, null));
    }

    /**
     * Gives this failure a number of the node's own, for a host that acts on which failure it
     * was without reading the words. Call it before throwing. The number means what this node's
     * author says: the host passes it through untouched, with the node's id.
     */
    public void faultCode(long code) {
        events.add(new Event("fault_code", 0f, Long.toString(code & 0xffffffffL), null));
    }

    List<Event> events() {
        return events;
    }

    boolean stateSet() {
        return stateSet;
    }

    /** Sets the inputs the body reads through the typed accessors. The worker calls this per node. */
    void inputs(Map<String, Object> values) {
        this.inputs = values;
    }
}
