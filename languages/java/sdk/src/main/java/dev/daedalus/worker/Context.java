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

    List<Event> events() {
        return events;
    }

    boolean stateSet() {
        return stateSet;
    }
}
