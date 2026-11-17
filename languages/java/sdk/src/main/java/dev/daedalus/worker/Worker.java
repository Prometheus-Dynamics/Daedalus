package dev.daedalus.worker;

import dev.daedalus.wire.Bytes;
import dev.daedalus.wire.Packets;
import dev.daedalus.wire.Types;
import dev.daedalus.wire.Types.Ty;
import dev.daedalus.wire.Values;
import dev.daedalus.wire.Values.Schema;
import java.io.ByteArrayInputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The worker loop. It reads host packets from the process's standard input and answers on standard
 * output, which is kept for packets: {@code System.out} is pointed at stderr, so node output cannot
 * corrupt the stream.
 *
 * <p>Two threads run the worker. A daemon reader thread owns standard input: it takes each packet as
 * it arrives, so a cancel reaches a running body while the body runs. A cancel names a run, and it
 * sets the flag {@link Context#cancelled()} reads when that run is the one going on. The loop thread
 * runs the nodes and writes the answers. The reader ends with the process: it is a daemon, and the
 * loop ends at a shutdown packet or when the input closes. Standard input is replaced by an empty
 * stream, so a node body cannot read the packets.
 *
 * <p>A body throws {@link NodeException} for a fault with its code. Any other throw is
 * {@code node_error}, whose reason is the stack cut to 512 bytes.
 */
public final class Worker {
    /** One node a class declares: its ports, its state type, and the method that runs it. */
    record Declared(String id, List<String> inputNames, List<Ty> inputs, List<String> outputNames,
                    List<Ty> outputs, Ty state, Method method) {}

    /** The end of the input, as the reader queues it. */
    private static final Object END = new Object();

    private final FileOutputStream packets;
    private final Map<String, Declared> byId = new HashMap<>();
    private final Map<String, Object> stateValues = new HashMap<>();
    private final Schema schema;
    private final List<Declared> declared;
    private final AtomicLong current = new AtomicLong(-1);
    private final AtomicBoolean cancelled = new AtomicBoolean();

    private Worker(FileOutputStream packets, List<Declared> declared, Set<String> codecs) {
        this.packets = packets;
        this.declared = declared;
        this.schema = new Schema(codecs);
        for (Declared node : declared) {
            byId.put(node.id(), node);
            for (int i = 0; i < node.inputNames().size(); i++) {
                schema.port(node.id(), "input", node.inputNames().get(i), node.inputs().get(i));
            }
            for (int i = 0; i < node.outputNames().size(); i++) {
                schema.port(node.id(), "output", node.outputNames().get(i), node.outputs().get(i));
            }
            if (node.state() != null) schema.port(node.id(), "state", "", node.state());
        }
    }

    /** Serves every {@code @Node} method of the given classes until the host shuts it down. */
    public static void serve(Class<?>... classes) throws IOException {
        FileOutputStream packets = new FileOutputStream(FileDescriptor.out);
        System.setOut(System.err);
        System.setIn(new ByteArrayInputStream(new byte[0]));
        List<Declared> nodes = new ArrayList<>();
        Set<String> codecs = new HashSet<>();
        for (Class<?> cls : classes) {
            for (Method method : cls.getDeclaredMethods()) {
                Node annotation = method.getAnnotation(Node.class);
                if (annotation == null) continue;
                if (!Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 2) {
                    throw new IllegalStateException(method + " must be static with (Map, Context)");
                }
                nodes.add(declare(annotation, method, codecs));
            }
        }
        Worker worker = new Worker(packets, nodes, codecs);
        LinkedBlockingQueue<Object> inbox = new LinkedBlockingQueue<>();
        startReader(new FileInputStream(FileDescriptor.in), inbox, worker.current, worker.cancelled);
        worker.loop(inbox);
    }

    private static Declared declare(Node annotation, Method method, Set<String> codecs) {
        List<String> inputNames = new ArrayList<>();
        List<Ty> inputs = new ArrayList<>();
        for (String port : annotation.inputs()) {
            Port parsed = Port.of(port);
            inputNames.add(parsed.name());
            inputs.add(parsed.ty());
            collectCodecs(parsed.ty(), codecs);
        }
        List<String> outputNames = new ArrayList<>();
        List<Ty> outputs = new ArrayList<>();
        for (String port : annotation.outputs()) {
            Port parsed = Port.of(port);
            outputNames.add(parsed.name());
            outputs.add(parsed.ty());
            collectCodecs(parsed.ty(), codecs);
        }
        Ty state = annotation.state().isEmpty() ? null : Types.parse(annotation.state());
        if (state != null) collectCodecs(state, codecs);
        method.setAccessible(true);
        return new Declared(annotation.id(), inputNames, inputs, outputNames, outputs, state, method);
    }

    /** A port written as {@code name:type}. */
    private record Port(String name, Ty ty) {
        static Port of(String text) {
            int colon = text.indexOf(':');
            if (colon < 0) throw new IllegalArgumentException("a port is name:type: " + text);
            return new Port(text.substring(0, colon).trim(), Types.parse(text.substring(colon + 1).trim()));
        }
    }

    private static void collectCodecs(Ty ty, Set<String> codecs) {
        if ("opaque".equals(ty.kind)) codecs.add(ty.key);
        ty.items.forEach(item -> collectCodecs(item, codecs));
        ty.payloads.forEach(payload -> { if (payload != null) collectCodecs(payload, codecs); });
    }

    /**
     * Starts the reader. It queues every packet in order, and it sets the cancel flag as a cancel
     * for the run going on arrives. It queues {@link #END} when the input ends or is broken.
     */
    private static void startReader(InputStream in, LinkedBlockingQueue<Object> inbox, AtomicLong current,
                                    AtomicBoolean cancelled) {
        Thread reader = new Thread(() -> {
            try {
                while (true) {
                    byte[] body = Packets.read(in, Packets.DEFAULT_CAP);
                    if (body == null) break;
                    if (body.length >= 9 && body[0] == Protocol.RUN) {
                        cancelled.set(false);
                        current.set(runOf(body));
                    } else if (body.length >= 9 && body[0] == Protocol.CANCEL && runOf(body) == current.get()) {
                        cancelled.set(true);
                    }
                    inbox.offer(body);
                }
            } catch (IOException e) {
                // The stream is broken: the loop ends, as it does at the end of the input.
            } finally {
                inbox.offer(END);
            }
        }, "daedalus-worker-reader");
        reader.setDaemon(true);
        reader.start();
    }

    /** The run id a run or cancel packet names: the u64 after its kind byte. */
    private static long runOf(byte[] body) {
        long run = 0;
        for (int i = 8; i >= 1; i--) run = (run << 8) | (body[i] & 0xFF);
        return run;
    }

    private void loop(LinkedBlockingQueue<Object> inbox) throws IOException {
        // The hello goes first.
        Bytes.Writer hello = new Bytes.Writer();
        hello.u8(Protocol.HELLO);
        hello.u32(Protocol.PROTOCOL_VERSION);
        hello.u64(Protocol.LAYOUT_HASH);
        hello.str("daedalus-java-worker");
        hello.u32(Packets.DEFAULT_CAP);
        hello.u32(declared.size());
        for (Declared node : declared) {
            hello.str(node.id());
            writePorts(hello, node.inputNames(), node.inputs());
            writePorts(hello, node.outputNames(), node.outputs());
            writePorts(hello, List.of(), List.of());
            if (node.state() == null) hello.u8(0);
            else {
                hello.u8(1);
                Types.encode(node.state(), hello, 0);
            }
            hello.u32(0);
        }
        send(hello.toByteArray());

        while (true) {
            Object item;
            try {
                item = inbox.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (item == END) return;
            byte[] body = (byte[]) item;
            Bytes.Reader reader = new Bytes.Reader(body);
            try {
                int kind = reader.u8();
                switch (kind) {
                    case Protocol.RUN -> runRun(reader);
                    case Protocol.PING -> send(new byte[] {(byte) Protocol.PONG});
                    case Protocol.SHUTDOWN -> { return; }
                    case Protocol.STATE_EXPORT -> stateExport(reader);
                    case Protocol.STATE_IMPORT -> stateImport(reader);
                    // A cancel is read by the reader thread. Between runs there is nothing to stop.
                    case Protocol.CANCEL -> { }
                    default -> sendFault(0, null, "malformed_packet", "unknown message kind " + kind);
                }
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                sendFault(0, null, "malformed_packet", String.valueOf(e.getMessage()));
            }
        }
    }

    private static void writePorts(Bytes.Writer out, List<String> names, List<Ty> types) {
        out.u32(names.size());
        for (int i = 0; i < names.size(); i++) {
            out.str(names.get(i));
            Types.encode(types.get(i), out, 0);
            out.u8(0);
        }
    }

    private void runRun(Bytes.Reader in) throws IOException {
        long run = in.u64();
        in.u32();
        int count = in.count();
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) nodes.add(in.str());
        int inputCount = in.count();
        Map<String, Map<String, Object>> inputs = new HashMap<>();
        for (int i = 0; i < inputCount; i++) {
            String node = in.str();
            String port = in.str();
            Object value = Values.decode(schema.lookup(node, "input", port), in, schema, 0);
            inputs.computeIfAbsent(node, k -> new LinkedHashMap<>()).put(port, value);
        }
        in.finish();

        List<Object[]> outputs = new ArrayList<>();
        int status = Protocol.OK;
        for (String id : nodes) {
            Declared node = byId.get(id);
            if (node == null) {
                sendFault(run, id, "unknown_node", "no node " + id);
                status = Protocol.FAULT_STATUS;
                break;
            }
            Map<String, Object> values = inputs.getOrDefault(id, Map.of());
            String missing = node.inputNames().stream().filter(name -> !values.containsKey(name)).findFirst().orElse(null);
            if (missing != null) {
                sendFault(run, id, "bad_input", id + " has no input " + missing);
                status = Protocol.FAULT_STATUS;
                break;
            }
            Context cx = new Context(stateValues.get(id), cancelled);
            cx.inputs(values);
            Object result;
            try {
                result = node.method().invoke(null, values, cx);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                flushEvents(run, cx);
                if (cause instanceof NodeException failure) sendFault(run, id, failure.code(), failure.getMessage());
                else sendFault(run, id, "node_error", stackOf(cause));
                status = Protocol.FAULT_STATUS;
                break;
            } catch (IllegalAccessException e) {
                flushEvents(run, cx);
                sendFault(run, id, "internal", String.valueOf(e));
                status = Protocol.FAULT_STATUS;
                break;
            }
            flushEvents(run, cx);
            if (node.state() != null && cx.stateSet()) stateValues.put(id, cx.state());
            Object[] values1;
            if (node.outputs().size() == 1) values1 = new Object[] {result};
            else if (node.outputs().isEmpty()) values1 = new Object[0];
            else if (result instanceof Object[] many && many.length == node.outputs().size()) values1 = many;
            else {
                sendFault(run, id, "internal", id + " returned the wrong number of values");
                status = Protocol.FAULT_STATUS;
                break;
            }
            for (int i = 0; i < values1.length; i++) {
                outputs.add(new Object[] {id, node.outputNames().get(i), values1[i]});
            }
            // A cancel that arrived while the node ran ends the run before the next node.
            if (cancelled.get()) break;
        }
        if (cancelled.get()) status = Protocol.CANCELLED;
        if (status == Protocol.FAULT_STATUS) outputs.clear();
        Bytes.Writer out = new Bytes.Writer();
        out.u8(Protocol.OUTPUTS);
        out.u64(run);
        out.u8(status);
        out.u32(outputs.size());
        for (Object[] entry : outputs) {
            String node = (String) entry[0];
            out.str(node);
            out.str((String) entry[1]);
            Values.encode(schema.lookup(node, "output", (String) entry[1]), entry[2], schema, out, 0);
        }
        send(out.toByteArray());
    }

    private void stateExport(Bytes.Reader in) throws IOException {
        String node = in.str();
        Declared declared = byId.get(node);
        // A node with no state, or with none set, has nothing to export: a fault names it, and the
        // value is never made up.
        if (declared == null || declared.state() == null || !stateValues.containsKey(node)) {
            sendFault(0, node, "bad_input", "the node has no state to export");
            return;
        }
        Bytes.Writer out = new Bytes.Writer();
        out.u8(Protocol.STATE_VALUE);
        out.str(node);
        Values.encode(declared.state(), stateValues.get(node), schema, out, 0);
        send(out.toByteArray());
    }

    private void stateImport(Bytes.Reader in) {
        String node = in.str();
        Object value = Values.decode(schema.lookup(node, "state", ""), in, schema, 0);
        stateValues.put(node, value);
    }

    private void flushEvents(long run, Context cx) throws IOException {
        for (Context.Event event : cx.events()) {
            Bytes.Writer out = new Bytes.Writer();
            switch (event.kind()) {
                case "progress" -> {
                    out.u8(Protocol.PROGRESS);
                    out.u64(run);
                    out.u32(0);
                    out.str(event.message());
                    out.u8(1);
                    out.f32(event.fraction());
                }
                case "fault_code" -> {
                    out.u8(Protocol.NODE_CODE);
                    out.u64(run);
                    out.u32(Long.parseLong(event.message()));
                }
                case "detail" -> {
                    out.u8(Protocol.DETAIL);
                    out.u64(run);
                    out.str(event.message());
                }
                default -> {
                    out.u8(Protocol.LOG);
                    out.u8(Protocol.LOG_LEVELS.get(event.level()));
                    out.str(event.message());
                }
            }
            send(out.toByteArray());
        }
    }

    private void sendFault(long run, String node, String code, String reason) throws IOException {
        Bytes.Writer out = new Bytes.Writer();
        out.u8(Protocol.FAULT);
        out.u64(run);
        if (node == null) out.u8(0);
        else {
            out.u8(1);
            out.str(node);
        }
        out.u16(Protocol.FAULT_CODES.get(code));
        out.str(truncate(reason, Protocol.REASON_CAP));
        send(out.toByteArray());
    }

    private void send(byte[] body) throws IOException {
        Packets.write(packets, body, Packets.DEFAULT_CAP);
    }

    private static String stackOf(Throwable error) {
        StringWriter text = new StringWriter();
        error.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    /** The text cut to at most {@code cap} bytes, at a character boundary. */
    static String truncate(String text, int cap) {
        byte[] data = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (data.length <= cap) return text;
        int cut = cap;
        while (cut > 0 && (data[cut] & 0xC0) == 0x80) cut--;
        return new String(data, 0, cut, java.nio.charset.StandardCharsets.UTF_8);
    }
}
