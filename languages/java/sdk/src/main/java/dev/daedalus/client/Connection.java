package dev.daedalus.client;

import dev.daedalus.service.Codec;
import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Messages.ClientMsg;
import dev.daedalus.service.Messages.ServerMsg;
import dev.daedalus.service.Ops;
import dev.daedalus.service.Ops.Op;
import dev.daedalus.service.Ops.WatchTarget;
import dev.daedalus.service.Protocol.EndReason;
import dev.daedalus.service.Protocol.Outcome;
import dev.daedalus.service.Protocol.ServiceHello;
import dev.daedalus.service.Protocol.ServiceError;
import dev.daedalus.service.Reply;
import dev.daedalus.wire.Packets;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One connection to the daemon: the hello, the requests waiting for answers, the watches, and a reader
 * thread that routes answers to their requests and events to their watches.
 *
 * <p>Two locks. The state lock guards the maps and the flags and is never held across a socket write;
 * the write lock keeps the packets of two requests apart. So a write that is slow to drain never blocks
 * the reader, and the reader never waits for a request that is being written.
 */
final class Connection {
    /** A request waiting for its answer. {@code answer} is null for a request nobody waits on. */
    private record Pending(CompletableFuture<Outcome> answer, boolean idempotent, Long watch) {}

    private final SocketChannel channel;
    private final InputStream in;
    private final OutputStream out;
    private final ServiceHello server;
    private final Set<String> codecs;
    private final long cap;
    private final Object stateLock = new Object();
    private final Object writeLock = new Object();
    private final Map<Long, Pending> pending = new HashMap<>();
    private final Map<Long, Watch> watches = new HashMap<>();
    private final Map<Long, Long> subs = new HashMap<>();
    private final AtomicLong ids = new AtomicLong(1);
    private boolean closed;

    private Connection(SocketChannel channel, ServiceHello server) {
        this.channel = channel;
        this.in = Channels.newInputStream(channel);
        this.out = Channels.newOutputStream(channel);
        this.server = server;
        this.codecs = Set.copyOf(server.capabilities().codecs());
        this.cap = Math.min(server.packetCap(), Packets.DEFAULT_CAP);
    }

    /** Connects and runs the hello, then starts the reader. */
    static Connection open(Endpoint endpoint) throws ClientException {
        SocketChannel channel = connect(endpoint);
        try {
            InputStream in = Channels.newInputStream(channel);
            OutputStream out = Channels.newOutputStream(channel);
            Packets.write(out, Codec.encodeClient(new ClientMsg.Hello(ServiceHello.client(Packets.DEFAULT_CAP)),
                Set.of()), Packets.DEFAULT_CAP);
            byte[] body = Packets.read(in, Packets.DEFAULT_CAP);
            if (body == null) throw new ClientException(ClientException.Kind.DISCONNECTED, "the server closed the connection");
            ServerMsg answer = Codec.decodeServer(body, Set.of());
            ServiceHello hello;
            switch (answer) {
                case ServerMsg.Hello h -> hello = h.hello();
                case ServerMsg.Response r when r.outcome() instanceof ServiceError error -> throw ClientException.service(error);
                default -> throw new ClientException(ClientException.Kind.UNEXPECTED, "the server did not answer the hello");
            }
            String refusal = hello.refusal();
            if (refusal != null) throw new ClientException(ClientException.Kind.REFUSED, refusal);
            Connection connection = new Connection(channel, hello);
            Thread reader = new Thread(connection::readLoop, "daedalus-client");
            reader.setDaemon(true);
            reader.start();
            return connection;
        } catch (IOException e) {
            closeQuietly(channel);
            throw new ClientException(ClientException.Kind.IO, e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            closeQuietly(channel);
            throw new ClientException(ClientException.Kind.WIRE, e.getMessage(), e);
        } catch (ClientException e) {
            closeQuietly(channel);
            throw e;
        }
    }

    private static SocketChannel connect(Endpoint endpoint) throws ClientException {
        try {
            return switch (endpoint) {
                case Endpoint.Unix unix -> {
                    SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
                    try {
                        channel.connect(UnixDomainSocketAddress.of(unix.path()));
                    } catch (IOException e) {
                        closeQuietly(channel);
                        throw e;
                    }
                    yield channel;
                }
                case Endpoint.Tcp tcp -> {
                    SocketChannel channel = SocketChannel.open(tcp.address());
                    channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                    yield channel;
                }
            };
        } catch (IOException e) {
            throw new ClientException(ClientException.Kind.IO, "cannot connect to " + endpoint + ": " + e.getMessage(), e);
        }
    }

    ServiceHello server() {
        return server;
    }

    boolean isClosed() {
        synchronized (stateLock) {
            return closed;
        }
    }

    /** Sends one request and waits for its outcome. A refusal comes back as a {@link ServiceError}. */
    Outcome call(String session, Op op) throws ClientException {
        return await(send(session, op, null, true));
    }

    /** Subscribes: the watch is in the table before its request goes out, so no event can miss it. */
    Watch subscribe(WatchTarget target, Ops.EventKinds kinds, String session) throws ClientException {
        long id = ids.getAndIncrement();
        Watch watch = new Watch(this, id, target, kinds, session);
        synchronized (stateLock) {
            if (closed) throw new ClientException(ClientException.Kind.DISCONNECTED, "disconnected");
            watches.put(id, watch);
        }
        try {
            Outcome outcome = await(send(session, new Ops.Watch(target, kinds), id, true));
            if (outcome instanceof Reply.Watching) return watch;
            throw new ClientException(ClientException.Kind.UNEXPECTED, "watch was answered with " + outcome);
        } catch (ClientException e) {
            synchronized (stateLock) {
                if (watches.remove(id) != null) watch.end();
            }
            throw e;
        }
    }

    /** Ends a watch: forgets it, and tells the host if it had a subscription. Events queued stay readable. */
    void unwatch(Watch watch) {
        Long sub;
        synchronized (stateLock) {
            if (watches.remove(watch.id()) == null) return;
            sub = watch.sub;
            if (sub != null) subs.remove(sub);
            watch.end();
        }
        if (sub != null && !isClosed()) {
            try {
                send(watch.session(), new Ops.Unwatch(sub), null, false);
            } catch (ClientException ignored) {
                // The watch is already forgotten here, and a host that has gone has ended it too.
            }
        }
    }

    /** Closes the socket. The reader ends, and every waiting call fails. */
    void close() {
        synchronized (stateLock) {
            if (closed) return;
            closed = true;
        }
        closeQuietly(channel);
    }

    private CompletableFuture<Outcome> send(String session, Op op, Long watch, boolean wait) throws ClientException {
        long id = ids.getAndIncrement();
        byte[] body;
        try {
            body = Codec.encodeClient(new ClientMsg.Request(id, session, op), codecs);
        } catch (IllegalArgumentException e) {
            throw new ClientException(ClientException.Kind.WIRE, e.getMessage(), e);
        }
        if (body.length > cap) {
            throw new ClientException(ClientException.Kind.TOO_LARGE,
                "a request of " + body.length + " bytes is over the server's cap of " + cap);
        }
        boolean idempotent = op.kind().idempotent();
        CompletableFuture<Outcome> answer = wait ? new CompletableFuture<>() : null;
        synchronized (stateLock) {
            if (closed) throw new ClientException(ClientException.Kind.DISCONNECTED, "disconnected");
            pending.put(id, new Pending(answer, idempotent, watch));
        }
        try {
            synchronized (writeLock) {
                Packets.write(out, body, cap);
            }
        } catch (IOException e) {
            synchronized (stateLock) {
                pending.remove(id);
            }
            close();
            // The request may have reached the host: only an idempotent one is safe to call lost.
            throw new ClientException(idempotent ? ClientException.Kind.DISCONNECTED : ClientException.Kind.UNKNOWN_DELIVERY,
                "the connection dropped while the request was written", e);
        }
        return answer;
    }

    private static Outcome await(CompletableFuture<Outcome> answer) throws ClientException {
        if (answer == null) throw new IllegalStateException("nothing waits for this request");
        try {
            Outcome outcome = answer.get();
            if (outcome instanceof ServiceError error) throw ClientException.service(error);
            return outcome;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ClientException failure) throw failure;
            throw new ClientException(ClientException.Kind.IO, String.valueOf(e.getCause()), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException(ClientException.Kind.INTERRUPTED, "interrupted while waiting for the answer", e);
        }
    }

    /** The reader thread: answers go to their requests, events to their watches, until the socket ends. */
    private void readLoop() {
        try {
            while (true) {
                byte[] body = Packets.read(in, Packets.DEFAULT_CAP);
                if (body == null) break;
                ServerMsg message = Codec.decodeServer(body, codecs);
                if (message instanceof ServerMsg.Response response) {
                    answer(response);
                } else if (message instanceof ServerMsg.EventMsg event) {
                    deliver(event.event());
                } else {
                    // A second hello: the connection cannot be trusted to stay in step.
                    break;
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            // The socket ended, or a message did not decode: the connection is dropped below.
        } finally {
            end();
        }
    }

    private void answer(ServerMsg.Response response) {
        CompletableFuture<Outcome> answer;
        synchronized (stateLock) {
            Pending waiting = pending.remove(response.id());
            if (waiting == null) return;
            answer = waiting.answer();
            if (waiting.watch() != null) {
                Watch watch = watches.get(waiting.watch());
                if (response.outcome() instanceof Reply.Watching watching && watch != null) {
                    // Recorded before the caller hears of it, and before the next packet is read, so no
                    // event of this subscription can arrive unknown.
                    watch.sub = watching.sub();
                    subs.put(watching.sub(), watch.id());
                } else {
                    // The host no longer has what the watch followed: its stream ends.
                    watches.remove(waiting.watch());
                    if (watch != null) watch.end();
                }
            }
        }
        if (answer != null) answer.complete(response.outcome());
    }

    private void deliver(Event event) {
        synchronized (stateLock) {
            Long id = subs.get(event.sub());
            if (id == null) return;
            Watch watch = watches.get(id);
            if (watch == null) return;
            // The last event of a subscription: a single-run watch's run ended, or the watch itself did.
            boolean last = false;
            if (event.body() instanceof dev.daedalus.service.Events.Ended ended) {
                last = watch.target() instanceof WatchTarget.OneRun || ended.run() == 0
                    || ended.reason() == EndReason.UNWATCHED || ended.reason() == EndReason.SHUTDOWN;
            }
            watch.deliver(new Event(id, event.body()));
            if (last) {
                watches.remove(id);
                subs.remove(event.sub());
                watch.end();
            }
        }
    }

    /** The socket ended: every waiting call fails, and every watch's stream ends. */
    private void end() {
        synchronized (stateLock) {
            closed = true;
            for (Pending waiting : pending.values()) {
                if (waiting.answer() != null) {
                    waiting.answer().completeExceptionally(new ClientException(
                        waiting.idempotent() ? ClientException.Kind.DISCONNECTED : ClientException.Kind.UNKNOWN_DELIVERY,
                        "the connection dropped before the answer"));
                }
            }
            pending.clear();
            for (Watch watch : watches.values()) watch.end();
            watches.clear();
            subs.clear();
        }
        closeQuietly(channel);
    }

    private static void closeQuietly(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // Closing a socket that is already gone is the goal, not an error.
        }
    }
}
