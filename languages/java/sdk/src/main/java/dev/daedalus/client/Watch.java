package dev.daedalus.client;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Events.Lagged;
import dev.daedalus.service.Ops.EventKinds;
import dev.daedalus.service.Ops.WatchTarget;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The events of one watch, in the order the host sent them. It is an {@link Iterator}: {@link #hasNext()}
 * waits until an event or the end has come. {@link #poll(Duration)} waits for a while only.
 *
 * <p>A watch keeps at most {@link #WATCH_QUEUE} unread events, as the host does for each watcher. An event
 * that comes to a full watch drops the oldest, and the next read is a {@link Lagged} with how many were
 * dropped, before anything else. The stream ends when the run it follows ends, when the watch is closed,
 * or when the connection drops: events already received can still be read after that. Closing the watch
 * asks the host to end it, and does not wait for the answer.
 *
 * <p>A watch is for one thread to read. Its {@link #close()} may be called from any thread.
 */
public final class Watch implements Iterator<Event>, AutoCloseable {
    /** The most unread events a watch keeps. */
    public static final int WATCH_QUEUE = 64;

    private final Connection connection;
    private final long id;
    private final WatchTarget target;
    private final EventKinds kinds;
    private final String session;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition ready = lock.newCondition();
    /** Guarded by {@link #lock}. */
    private final ArrayDeque<Event> events = new ArrayDeque<>(WATCH_QUEUE);
    /** Events dropped since the last read. Guarded by {@link #lock}. */
    private long dropped;
    /** No event will be added. Guarded by {@link #lock}. */
    private boolean ended;
    /** The host's number for the subscription, once the host has answered it. Guarded by the connection. */
    Long sub;
    /** An event read by {@link #hasNext()} and not yet given out. */
    private Event head;

    Watch(Connection connection, long id, WatchTarget target, EventKinds kinds, String session) {
        this.connection = connection;
        this.id = id;
        this.target = target;
        this.kinds = kinds;
        this.session = session;
    }

    /** The watch's number on this client. Every event carries it, whatever number the host gave. */
    public long id() {
        return id;
    }

    WatchTarget target() {
        return target;
    }

    EventKinds kinds() {
        return kinds;
    }

    String session() {
        return session;
    }

    /** Adds an event, dropping the oldest when the watch is full. */
    void deliver(Event event) {
        lock.lock();
        try {
            if (ended) return;
            if (events.size() == WATCH_QUEUE) {
                events.pollFirst();
                dropped++;
            }
            events.addLast(event);
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Ends the stream: what is queued stays readable, and nothing more is added. */
    void end() {
        lock.lock();
        try {
            ended = true;
            ready.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** The next event, waiting up to {@code nanos} (forever when negative). Null at the end or the timeout. */
    private Event take(long nanos) throws InterruptedException {
        boolean forever = nanos < 0;
        lock.lock();
        try {
            for (;;) {
                if (dropped > 0) {
                    Event lagged = new Event(id, new Lagged(dropped));
                    dropped = 0;
                    return lagged;
                }
                Event event = events.pollFirst();
                if (event != null) return event;
                if (ended) return null;
                if (forever) {
                    ready.await();
                } else {
                    // awaitNanos gives what is left, which is zero or less once the time is up.
                    if (nanos <= 0) return null;
                    nanos = ready.awaitNanos(nanos);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean isEnded() {
        lock.lock();
        try {
            return ended && dropped == 0 && events.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits for the next event, or for the end. Returns false once the stream has ended; an interrupt
     * while waiting also returns false, with the thread's interrupt flag set again.
     */
    @Override
    public boolean hasNext() {
        if (head == null) {
            try {
                head = take(-1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return head != null;
    }

    /** The next event. Throws {@link NoSuchElementException} once the stream has ended. */
    @Override
    public Event next() {
        if (!hasNext()) throw new NoSuchElementException("watch " + id + " has ended");
        Event event = head;
        head = null;
        return event;
    }

    /** Waits up to {@code timeout} for the next event, the end, or neither. */
    public Poll poll(Duration timeout) {
        if (head == null) {
            try {
                head = take(Math.max(0, timeout.toNanos()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new Poll.Timeout();
            }
            if (head == null) return isEnded() ? new Poll.Ended() : new Poll.Timeout();
        }
        Event event = head;
        head = null;
        return new Poll.Got(event);
    }

    /** Ends the watch on the host. Events already received can still be read. */
    @Override
    public void close() {
        connection.unwatch(this);
    }
}
