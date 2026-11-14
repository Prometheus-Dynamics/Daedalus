package dev.daedalus.worker;

import java.util.ArrayList;
import java.util.List;

/** What a node body can see and report. Events go out, in order, before the run's outputs. */
public final class Context {
    /** One event a body reported. */
    public record Event(String kind, float fraction, String message, String level) {}

    private Object state;
    private boolean stateSet;
    private final List<Event> events = new ArrayList<>();

    Context(Object state) {
        this.state = state;
    }

    public boolean cancelled() {
        return false;
    }

    /** The node's state from its last run, or null. */
    public Object state() {
        return state;
    }

    public void setState(Object value) {
        this.state = value;
        this.stateSet = true;
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
}
