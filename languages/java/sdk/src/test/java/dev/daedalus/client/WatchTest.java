package dev.daedalus.client;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Events.Lagged;
import dev.daedalus.service.Events.Telemetry;
import java.time.Duration;

/**
 * A watch's bound, as a plain program: {@code java -cp <classes> dev.daedalus.client.WatchTest}. A watch
 * keeps at most {@link Watch#WATCH_QUEUE} unread events; past that the oldest go, and the next read is a
 * {@link Lagged} with their count. What was queued before the end stays readable. It exits non-zero on the
 * first failed check.
 */
public final class WatchTest {
    private WatchTest() {}

    static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError("failed: " + what);
    }

    static Event telemetry(long tick) {
        return new Event(7, new Telemetry(1, tick, 0, 0, 0));
    }

    public static void main(String[] args) {
        Watch watch = new Watch(null, 7, null, null, "default");
        int extra = 5;
        for (long tick = 0; tick < Watch.WATCH_QUEUE + extra; tick++) watch.deliver(telemetry(tick));
        check(watch.next().equals(new Event(7, new Lagged(extra))), "the count of dropped events comes first");
        for (long tick = extra; tick < Watch.WATCH_QUEUE + extra; tick++) {
            check(watch.next().equals(telemetry(tick)), "the newest events stay, in order: " + tick);
        }
        check(watch.poll(Duration.ofMillis(10)) instanceof Poll.Timeout, "an empty open watch times out");
        watch.deliver(telemetry(100));
        watch.end();
        check(watch.poll(Duration.ZERO).equals(new Poll.Got(telemetry(100))), "an event queued before the end is read");
        check(watch.poll(Duration.ZERO) instanceof Poll.Ended, "then the watch has ended");
        check(!watch.hasNext(), "and the iterator is done");
        System.out.println("watch checks passed");
    }
}
