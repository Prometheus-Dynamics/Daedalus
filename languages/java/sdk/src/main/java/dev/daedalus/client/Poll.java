package dev.daedalus.client;

import dev.daedalus.service.Events.Event;

/** What a timed wait for the next event of a watch found. */
public sealed interface Poll permits Poll.Got, Poll.Timeout, Poll.Ended {
    /** The next event, in the order the host sent them. */
    record Got(Event event) implements Poll {}

    /** Nothing came in time. The watch is still open. */
    record Timeout() implements Poll {}

    /** The watch has ended, and no event will come. */
    record Ended() implements Poll {}
}
