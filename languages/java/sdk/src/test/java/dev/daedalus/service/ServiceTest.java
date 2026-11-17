package dev.daedalus.service;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Events.Fault;
import dev.daedalus.service.Events.Unknown;
import dev.daedalus.service.Messages.ClientMsg;
import dev.daedalus.service.Messages.ServerMsg;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Protocol.ErrorCode;
import dev.daedalus.service.Protocol.OpKind;
import dev.daedalus.service.Protocol.ServiceError;
import dev.daedalus.service.Protocol.ServiceHello;
import dev.daedalus.wire.Bytes;
import dev.daedalus.wire.Packets;
import dev.daedalus.wire.Packets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The service codec's checks, as a plain program: {@code java -cp <classes> dev.daedalus.service.ServiceTest
 * <service layout hash>}. The edge cases of the additive rules: truncated input, an event kind and an
 * error code this side does not have, trailing bytes after a message and inside a record, an op this
 * side does not have, and a hello refused for its layout. It exits non-zero on the first failed check.
 */
public final class ServiceTest {
    private ServiceTest() {}

    static void check(boolean condition, String what) {
        if (!condition) throw new AssertionError("failed: " + what);
    }

    static boolean refused(Runnable action) {
        try {
            action.run();
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    static byte[] bytesOf(byte kind, long id) {
        Bytes.Writer out = new Bytes.Writer();
        out.u8(kind);
        out.u64(id);
        return out.toByteArray();
    }

    public static void main(String[] args) throws IOException {
        // A client's hello and a request round trip, and the request's id is read back.
        ServiceHello hello = ServiceHello.client(Packets.DEFAULT_CAP);
        byte[] helloBody = Codec.encodeClient(new ClientMsg.Hello(hello), Set.of());
        check(Codec.decodeClient(helloBody, Set.of()).equals(new ClientMsg.Hello(hello)), "a hello round trips");
        byte[] request = Codec.encodeClient(new ClientMsg.Request(77, "default", new Ops.Ping()), Set.of());
        check(Codec.requestId(request) == 77, "the id of a request is read from its envelope");
        check(Codec.decodeClient(request, Set.of()).equals(new ClientMsg.Request(77, "default", new Ops.Ping())),
            "a request round trips");

        // Truncated input is refused, and a packet cut short is an I/O error.
        byte[] reply = Codec.encodeServer(new ServerMsg.Response(3, new Reply.Pong(1)), Set.of());
        byte[] cut = java.util.Arrays.copyOf(reply, reply.length - 2);
        check(refused(() -> Codec.decodeServer(cut, Set.of())), "a truncated response is refused");
        check(refused(() -> Codec.decodeServer(new byte[0], Set.of())), "an empty body is refused");
        byte[] packet = {10, 0, 0, 0, 1, 2};
        boolean ioError = false;
        try {
            Packets.read(new ByteArrayInputStream(packet), Packets.DEFAULT_CAP);
        } catch (IOException expected) {
            ioError = true;
        }
        check(ioError, "a packet cut short is an I/O error");

        // An event kind this side does not have is Unknown, with its own fields skipped.
        Bytes.Writer unknown = new Bytes.Writer();
        unknown.u8(Protocol.EVENT);
        unknown.u64(9);
        unknown.u8(77);
        unknown.bytes(new byte[] {5, 6, 7});
        ServerMsg event = Codec.decodeServer(unknown.toByteArray(), Set.of());
        check(event.equals(new ServerMsg.EventMsg(new Event(9, new Unknown(77)))), "an unknown event kind is Unknown");

        // An error code this side does not have is OTHER, and the message is kept.
        Bytes.Writer error = new Bytes.Writer();
        error.u8(Protocol.RESPONSE);
        error.u64(3);
        error.u8(1);
        error.u16(900);
        error.str("the pool is full");
        error.u32(0);
        ServerMsg refusedReply = Codec.decodeServer(error.toByteArray(), Set.of());
        check(refusedReply.equals(new ServerMsg.Response(3, new ServiceError(ErrorCode.OTHER, "the pool is full"))),
            "an unknown error code is other, with its message");

        // Trailing bytes after a whole message are ignored: a newer side may put fields there.
        byte[] padded = java.util.Arrays.copyOf(reply, reply.length + 5);
        check(Codec.decodeServer(padded, Set.of()).equals(new ServerMsg.Response(3, new Reply.Pong(1))),
            "trailing bytes after a message are skipped");

        // Trailing bytes inside a record are skipped by its reader, and the record's fields still read.
        Bytes.Writer graph = new Bytes.Writer();
        graph.u8(Protocol.RESPONSE);
        graph.u64(4);
        graph.u8(0);
        graph.u8(3);
        graph.str("pass");
        graph.u32(1);
        graph.record(w -> {
            w.u8(1);
            w.str("W1");
            w.str("a warning");
            w.u8(0);
            w.u8(0);
            w.u8(0xEE);
            w.u8(0xEE);
        });
        ServerMsg withRecord = Codec.decodeServer(graph.toByteArray(), Set.of());
        check(withRecord.equals(new ServerMsg.Response(4, new Reply.Graph("pass", List.of(
            new Protocol.Diagnostic(Protocol.Severity.WARNING, "W1", "a warning", null, null))))),
            "a record's trailing bytes are skipped");

        // A fault written before the code fields existed reads as code zero, not the host's own.
        Bytes.Writer oldFault = new Bytes.Writer();
        oldFault.u8(Protocol.EVENT);
        oldFault.u64(2);
        oldFault.u8(0);
        oldFault.u64(1);
        oldFault.u32(1);
        oldFault.u32(0);
        oldFault.u8(0);
        oldFault.u32(0);
        oldFault.u32(1);
        oldFault.str("boom");
        oldFault.u8(0);
        oldFault.str("");
        oldFault.u8(0);
        ServerMsg old = Codec.decodeServer(oldFault.toByteArray(), Set.of());
        check(old instanceof ServerMsg.EventMsg e && e.event().body() instanceof Fault f && f.code() == 0 && !f.ownCode(),
            "a fault without its code fields reads as zero");

        // An op this side does not have is refused by name, and a list of ops drops it.
        Bytes.Writer badOp = new Bytes.Writer();
        badOp.u8(Protocol.REQUEST);
        badOp.u64(5);
        badOp.str("default");
        badOp.u8(200);
        check(refused(() -> Codec.decodeClient(badOp.toByteArray(), Set.of())), "an op from the future is refused");
        check(OpKind.named(200) == OpKind.OTHER, "a name for an op this side lacks is other");

        // A refusal names the layout and the version, and a hello the client does not know is refused.
        ServiceHello wrongLayout = new ServiceHello(1, hello.layout() ^ 1, 16777216,
            hello.capabilities(), List.of(), List.of());
        check(wrongLayout.refusal() != null && wrongLayout.refusal().contains("layout"), "another layout is refused");
        ServiceHello needy = new ServiceHello(1, hello.layout(), 16777216, hello.capabilities(), List.of(),
            List.of("run.priority"));
        check(needy.refusal() != null && needy.refusal().contains("run.priority"), "an unknown need is refused by name");
        check(hello.refusal() == null, "this side's own hello is accepted");

        // A telemetry row written before its timing fields reads them as zero.
        Bytes.Writer telemetry = new Bytes.Writer();
        telemetry.u8(Protocol.RESPONSE);
        telemetry.u64(6);
        telemetry.u8(0);
        telemetry.u8(12);
        telemetry.u8(3);
        telemetry.u64(1);
        telemetry.u8(2);
        telemetry.u64(3);
        telemetry.u64(4);
        telemetry.u64(5);
        telemetry.u64(6);
        telemetry.u32(1);
        telemetry.record(w -> {
            w.u32(0);
            w.u32(1);
            w.str("work");
            w.u32(1);
            w.u32(0);
            w.u8(0);
            w.u8(0);
            w.u8(0);
        });
        Reply.Inspected inspected = (Reply.Inspected) ((ServerMsg.Response) Codec.decodeServer(telemetry.toByteArray(), Set.of())).outcome();
        check(inspected.inspection() instanceof Inspection.Telemetry t && t.rows().get(0).timeNs() == 0,
            "a row without timing reads them as zero");

        // The session names are refused when they are empty or over the bound.
        check(refused(() -> Codec.encodeClient(new ClientMsg.Request(1, "x".repeat(65), new Ops.Ping()), Set.of())),
            "a session name over the bound is refused");

        if (args.length > 0) {
            check(Protocol.LAYOUT_HASH == Long.parseUnsignedLong(args[0]), "the service layout hash is the Rust constant");
        }
        System.out.println("ServiceTest: all checks pass");
    }
}
