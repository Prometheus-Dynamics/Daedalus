package dev.daedalus.service;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Ops.Op;
import dev.daedalus.service.Protocol.Outcome;
import dev.daedalus.service.Protocol.ServiceHello;

/** The messages each side sends: a client's hello and requests, a server's hello, answers and events. */
public final class Messages {
    private Messages() {}

    /** What a client sends. */
    public sealed interface ClientMsg permits ClientMsg.Hello, ClientMsg.Request {
        /** The client's hello. */
        record Hello(ServiceHello hello) implements ClientMsg {}

        /** One request in its envelope: its id, the session its graph names are in, and its op. */
        record Request(long id, String session, Op op) implements ClientMsg {}
    }

    /** What a server sends. */
    public sealed interface ServerMsg permits ServerMsg.Hello, ServerMsg.Response, ServerMsg.EventMsg {
        record Hello(ServiceHello hello) implements ServerMsg {}

        /** The answer to the request with {@code id}. */
        record Response(long id, Outcome outcome) implements ServerMsg {}

        /** One event of the watch with number {@code event.sub}. */
        record EventMsg(Event event) implements ServerMsg {}
    }
}
