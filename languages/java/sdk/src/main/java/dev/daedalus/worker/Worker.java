package dev.daedalus.worker;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
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

/**
 * The worker loop. It reads host packets from stdin and answers on the original stdout, which is
 * kept on file descriptor 1 for packets: {@code System.out} is pointed at stderr, so node output
 * cannot corrupt the stream. A body throws {@link NodeException} for a fault with its code; any
 * other throw is {@code node_error}, whose reason is the stack cut to 512 bytes.
 */
public final class Worker {
    /** One node a class declares: its ports, its state type, and the method that runs it. */
    record Declared(String id, List<String> inputNames, List<Wire.Ty> inputs, List<String> outputNames,
                    List<Wire.Ty> outputs, Wire.Ty state, Method method) {}

    private final FileOutputStream packets;
    private final Map<String, Declared> byId = new HashMap<>();
    private final Map<String, Object> stateValues = new HashMap<>();
    private final Wire.Schema schema;
    private final List<Declared> declared;

    private Worker(FileOutputStream packets, List<Declared> declared, Set<String> codecs) {
        this.packets = packets;
        this.declared = declared;
        this.schema = new Wire.Schema(codecs);
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
        new Worker(packets, nodes, codecs).loop(new BufferedInputStream(new FileInputStream(FileDescriptor.in)));
    }

    private static Declared declare(Node annotation, Method method, Set<String> codecs) {
        List<String> inputNames = new ArrayList<>();
        List<Wire.Ty> inputs = new ArrayList<>();
        for (String port : annotation.inputs()) {
            Port parsed = Port.of(port);
            inputNames.add(parsed.name());
            inputs.add(parsed.ty());
            collectCodecs(parsed.ty(), codecs);
        }
        List<String> outputNames = new ArrayList<>();
        List<Wire.Ty> outputs = new ArrayList<>();
        for (String port : annotation.outputs()) {
            Port parsed = Port.of(port);
            outputNames.add(parsed.name());
            outputs.add(parsed.ty());
            collectCodecs(parsed.ty(), codecs);
        }
        Wire.Ty state = annotation.state().isEmpty() ? null : Wire.parseType(annotation.state());
        if (state != null) collectCodecs(state, codecs);
        method.setAccessible(true);
        return new Declared(annotation.id(), inputNames, inputs, outputNames, outputs, state, method);
    }

    /** A port written as {@code name:type}. */
    private record Port(String name, Wire.Ty ty) {
        static Port of(String text) {
            int colon = text.indexOf(':');
            if (colon < 0) throw new IllegalArgumentException("a port is name:type: " + text);
            return new Port(text.substring(0, colon).trim(), Wire.parseType(text.substring(colon + 1).trim()));
        }
    }

    private static void collectCodecs(Wire.Ty ty, Set<String> codecs) {
        if ("opaque".equals(ty.kind)) codecs.add(ty.key);
        ty.items.forEach(item -> collectCodecs(item, codecs));
        ty.payloads.forEach(payload -> { if (payload != null) collectCodecs(payload, codecs); });
    }

    private void loop(BufferedInputStream in) throws IOException {
        // The hello goes first.
        Wire.Writer hello = new Wire.Writer();
        hello.u8(Wire.HELLO);
        hello.u32(Wire.PROTOCOL_VERSION);
        hello.u64(new java.math.BigInteger(Long.toUnsignedString(Wire.LAYOUT_HASH)));
        hello.str("daedalus-java-worker");
        hello.u32(Wire.DEFAULT_CAP);
        hello.u32(declared.size());
        for (Declared node : declared) {
            hello.str(node.id());
            writePorts(hello, node.inputNames(), node.inputs());
            writePorts(hello, node.outputNames(), node.outputs());
            writePorts(hello, List.of(), List.of());
            if (node.state() == null) hello.u8(0);
            else { hello.u8(1); Wire.encodeType(node.state(), hello, 0); }
            hello.u32(0);
        }
        send(hello.toByteArray());

        while (true) {
            byte[] body;
            try {
                body = Wire.readPacket(in, Wire.DEFAULT_CAP);
            } catch (IOException e) {
                return;
            }
            if (body == null) return;
            Wire.Reader reader = new Wire.Reader(body);
            int kind;
            try {
                kind = reader.u8();
                switch (kind) {
                    case Wire.RUN -> runRun(reader);
                    case Wire.PING -> send(new byte[] {(byte) Wire.PONG});
                    case Wire.SHUTDOWN -> { return; }
                    case Wire.STATE_EXPORT -> stateExport(reader);
                    case Wire.STATE_IMPORT -> stateImport(reader);
                    case Wire.CANCEL -> { }
                    default -> sendFault(0, null, "malformed_packet", "unknown message kind " + kind);
                }
            } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
                sendFault(0, null, "malformed_packet", String.valueOf(e.getMessage()));
            }
        }
    }

    private static void writePorts(Wire.Writer out, List<String> names, List<Wire.Ty> types) {
        out.u32(names.size());
        for (int i = 0; i < names.size(); i++) {
            out.str(names.get(i));
            Wire.encodeType(types.get(i), out, 0);
            out.u8(0);
        }
    }

    private void runRun(Wire.Reader in) throws IOException {
        long run = in.u64().longValue();
        in.u32();
        int count = in.count();
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) nodes.add(in.str());
        int inputCount = in.count();
        Map<String, Map<String, Object>> inputs = new HashMap<>();
        for (int i = 0; i < inputCount; i++) {
            String node = in.str();
            String port = in.str();
            Object value = Wire.decodeValue(schema.lookup(node, "input", port), in, schema, 0);
            inputs.computeIfAbsent(node, k -> new LinkedHashMap<>()).put(port, value);
        }
        in.finish();

        List<Object[]> outputs = new ArrayList<>();
        String status = "ok";
        for (String id : nodes) {
            Declared node = byId.get(id);
            if (node == null) {
                sendFault(run, id, "unknown_node", "no node " + id);
                status = "fault";
                break;
            }
            Map<String, Object> values = inputs.getOrDefault(id, Map.of());
            String missing = node.inputNames().stream().filter(name -> !values.containsKey(name)).findFirst().orElse(null);
            if (missing != null) {
                sendFault(run, id, "bad_input", id + " has no input " + missing);
                status = "fault";
                break;
            }
            Context cx = new Context(stateValues.get(id));
            Object result;
            try {
                result = node.method().invoke(null, values, cx);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                flushEvents(run, cx);
                if (cause instanceof NodeException failure) sendFault(run, id, failure.code(), failure.getMessage());
                else sendFault(run, id, "node_error", stackOf(cause));
                status = "fault";
                break;
            } catch (IllegalAccessException e) {
                flushEvents(run, cx);
                sendFault(run, id, "internal", String.valueOf(e));
                status = "fault";
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
                status = "fault";
                break;
            }
            for (int i = 0; i < values1.length; i++) {
                outputs.add(new Object[] {id, node.outputNames().get(i), values1[i]});
            }
        }
        if (status.equals("fault")) outputs.clear();
        Wire.Writer out = new Wire.Writer();
        out.u8(Wire.OUTPUTS);
        out.u64(new java.math.BigInteger(Long.toString(run)));
        out.u8(status.equals("ok") ? 0 : 2);
        out.u32(outputs.size());
        for (Object[] entry : outputs) {
            String node = (String) entry[0];
            out.str(node);
            out.str((String) entry[1]);
            Wire.encodeValue(schema.lookup(node, "output", (String) entry[1]), entry[2], schema, out, 0);
        }
        send(out.toByteArray());
    }

    private void stateExport(Wire.Reader in) throws IOException {
        String node = in.str();
        Declared declared = byId.get(node);
        if (declared == null || declared.state() == null) return;
        Wire.Writer out = new Wire.Writer();
        out.u8(Wire.STATE_VALUE);
        out.str(node);
        Wire.encodeValue(declared.state(), stateValues.getOrDefault(node, 0L), schema, out, 0);
        send(out.toByteArray());
    }

    private void stateImport(Wire.Reader in) {
        String node = in.str();
        Object value = Wire.decodeValue(schema.lookup(node, "state", ""), in, schema, 0);
        stateValues.put(node, value);
    }

    private void flushEvents(long run, Context cx) throws IOException {
        for (Context.Event event : cx.events()) {
            Wire.Writer out = new Wire.Writer();
            if (event.kind().equals("progress")) {
                out.u8(Wire.PROGRESS);
                out.u64(new java.math.BigInteger(Long.toString(run)));
                out.u32(0);
                out.str(event.message());
                out.u8(1);
                out.f32(event.fraction());
            } else {
                out.u8(Wire.LOG);
                out.u8(Wire.LOG_LEVELS.get(event.level()));
                out.str(event.message());
            }
            send(out.toByteArray());
        }
    }

    private void sendFault(long run, String node, String code, String reason) throws IOException {
        Wire.Writer out = new Wire.Writer();
        out.u8(Wire.FAULT);
        out.u64(new java.math.BigInteger(Long.toString(run)));
        if (node == null) out.u8(0);
        else { out.u8(1); out.str(node); }
        out.u16(Wire.FAULT_CODES.get(code));
        out.str(Wire.truncate(reason, Wire.REASON_CAP));
        send(out.toByteArray());
    }

    private void send(byte[] body) throws IOException {
        Wire.writePacket(packets, body, Wire.DEFAULT_CAP);
    }

    private static String stackOf(Throwable error) {
        StringWriter text = new StringWriter();
        error.printStackTrace(new PrintWriter(text));
        return text.toString();
    }
}
