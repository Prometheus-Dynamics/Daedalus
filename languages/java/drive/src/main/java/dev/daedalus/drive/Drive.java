package dev.daedalus.drive;

import dev.daedalus.client.Client;
import dev.daedalus.client.ClientException;
import dev.daedalus.client.Endpoint;
import dev.daedalus.client.Run;
import dev.daedalus.client.Watch;
import dev.daedalus.service.Codec;
import dev.daedalus.service.Ops.EventKinds;
import dev.daedalus.service.Ops.InspectTarget;
import dev.daedalus.service.Ops.PlanOptions;
import dev.daedalus.service.Ops.StartConfig;
import dev.daedalus.service.Ops.WatchTarget;
import dev.daedalus.service.Protocol;
import dev.daedalus.service.Protocol.PlanForm;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.Protocol.TelemetryLevel;
import dev.daedalus.service.TypedValue;
import dev.daedalus.wire.Types;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The Java driver of the drive conformance suite. The contract is the crate doc of
 * {@code testing/crates/drive-conformance}: one answer line per command line on standard input, and the
 * recode mode {@code --recode client|server FILE} over the corpus.
 *
 * <p>Usage: {@code Drive ENDPOINT}, or {@code Drive --recode client|server FILE}.
 */
public final class Drive {
    /** A command that gives no answer but a line: an unreadable line, or a name never made. */
    private static final class Usage extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /** A failure that is not the host's: a file that cannot be read. */
    private static final class Local extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private final Client root;
    private Client current;
    private final Map<Long, Watch> watches = new HashMap<>();
    private final Map<String, byte[]> blobs = new HashMap<>();

    private Drive(Client root) {
        this.root = root;
        this.current = root;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 3 && "--recode".equals(args[0])) {
            recode(args[1], Path.of(args[2]));
            return;
        }
        if (args.length != 1) {
            System.err.println("usage: Drive ENDPOINT | Drive --recode client|server FILE");
            System.exit(2);
        }
        Client client;
        try {
            client = Client.connect(Endpoint.parse(args[0]));
        } catch (ClientException | IllegalArgumentException e) {
            System.err.println("cannot connect: " + e.getMessage());
            System.exit(1);
            return;
        }
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        Drive driver = new Drive(client);
        String line;
        while ((line = in.readLine()) != null) {
            out.write(driver.answer(line));
            out.write("\n");
        }
        out.flush();
        client.close();
        System.exit(0);
    }

    /** The one output line for one input line. */
    String answer(String line) {
        try {
            return run(line);
        } catch (Usage e) {
            return "error usage";
        } catch (Local e) {
            return "error local";
        } catch (ClientException e) {
            if (e.kind() == ClientException.Kind.SERVICE && e.code() != null) return Show.error(e.code());
            return "error local";
        } catch (IllegalArgumentException e) {
            return "error usage";
        }
    }

    private String run(String line) throws ClientException, Usage, Local {
        String[] words = line.split(" ", -1);
        String verb = words[0];
        switch (verb) {
            case "ping":
                expect(words, 1);
                return Show.pong(current.ping());
            case "session": {
                if (words.length > 2) throw new Usage();
                String name = words.length == 2 && !words[1].isEmpty() ? words[1] : Protocol.DEFAULT_SESSION;
                current = root.inSession(name);
                return Show.session(name);
            }
            case "load_plugin":
                expect(words, 2);
                return Show.plugin(current.loadPlugin(words[1]));
            case "put_graph": {
                expect(words, 3);
                String json = readFile(words[2]);
                return Show.graph(words[1], current.putGraph(words[1], json));
            }
            case "patch_graph": {
                expect(words, 3);
                String patch = readFile(words[2]);
                return Show.graph(words[1], current.patchGraph(words[1], patch));
            }
            case "plan":
                if (words.length < 3) throw new Usage();
                return Show.plan(current.plan(words[1], planOptions(words)));
            case "blob": {
                expect(words, 3);
                byte[] bytes = unhex(words[2]);
                blobs.put(words[1], bytes);
                return "blob name=" + Show.quote(words[1]) + " bytes=" + bytes.length;
            }
            case "make_plan": {
                if (words.length < 3) throw new Usage();
                StartConfig config = startConfig(words, 3);
                Client.MadePlan made = current.makePlan(words[1], config);
                blobs.put(words[2], made.bytes());
                return Show.made(made.hash(), made.bytes().length);
            }
            case "put_plan": {
                expect(words, 3);
                byte[] bytes = blobs.get(words[2]);
                if (bytes == null) throw new Usage();
                current.putPlan(words[1], bytes);
                return Show.ack();
            }
            case "start": {
                if (words.length < 2) throw new Usage();
                Run run = current.start(words[1], startConfig(words, 2));
                return Show.started(run.id());
            }
            case "feed": {
                String[] parts = line.split(" ", 4);
                if (parts.length < 4) throw new Usage();
                TypedValue value = parseValue(parts[3]);
                Client.Fed fed = current.feed(u64(parts[1]), parts[2], value);
                return Show.fed(fed.woke(), fed.dropped());
            }
            case "step":
                expect(words, 3);
                return Show.stepped(current.step(u64(words[1]), u32(words[2])));
            case "take": {
                expect(words, 3);
                Optional<TypedValue> value = current.take(u64(words[1]), words[2]);
                return Show.taken(value.orElse(null));
            }
            case "watch": {
                if (words.length < 3) throw new Usage();
                long id = u64(words[1]);
                WatchTarget target = "all".equals(words[2]) ? new WatchTarget.Everything()
                    : new WatchTarget.OneRun(u64(words[2]));
                Watch watch = current.watch(target, eventKinds(words));
                Watch replaced = watches.put(id, watch);
                if (replaced != null) replaced.close();
                return Show.watching(id);
            }
            case "next": {
                expect(words, 2);
                long id = u64(words[1]);
                Watch watch = watches.get(id);
                if (watch == null) throw new Usage();
                if (!watch.hasNext()) return Show.streamEnded(id);
                return Show.event(id, watch.next());
            }
            case "unwatch": {
                expect(words, 2);
                long id = u64(words[1]);
                Watch watch = watches.get(id);
                if (watch == null) throw new Usage();
                watch.close();
                return Show.unwatched(id);
            }
            case "reload":
                expect(words, 3);
                return Show.reloaded(current.reload(u64(words[1]), words[2]));
            case "cancel":
                expect(words, 2);
                current.cancel(u64(words[1]));
                return Show.ack();
            case "stop": {
                expect(words, 3);
                StopMode mode = switch (words[2]) {
                    case "drain" -> StopMode.DRAIN;
                    case "immediate" -> StopMode.IMMEDIATE;
                    default -> throw new Usage();
                };
                Client.Stopped stopped = current.stop(u64(words[1]), mode);
                return Show.stopped(stopped.tick(), stopped.dropped());
            }
            case "inspect":
                return inspect(words);
            case "cache":
                expect(words, 2);
                if (!"clear".equals(words[1])) throw new Usage();
                current.cacheClear();
                return Show.ack();
            case "trace":
                expect(words, 2);
                return Show.trace(current.trace(u32(words[1])));
            default:
                throw new Usage();
        }
    }

    private String inspect(String[] words) throws ClientException, Usage {
        if (words.length < 2) throw new Usage();
        switch (words[1]) {
            case "run":
                expect(words, 3);
                return Show.inspection(current.inspectRun(u64(words[2])));
            case "telemetry":
                expect(words, 3);
                return Show.inspection(current.inspectTelemetry(u64(words[2])));
            case "graph":
                expect(words, 3);
                return Show.inspection(current.inspect(new InspectTarget.OfGraph(words[2])));
            case "cache":
                expect(words, 2);
                return Show.cache(current.cacheInfo());
            case "host":
                expect(words, 2);
                return Show.inspection(current.inspect(new InspectTarget.OfHost()));
            default:
                throw new Usage();
        }
    }

    private static void expect(String[] words, int count) throws Usage {
        if (words.length != count) throw new Usage();
    }

    private static String readFile(String file) throws Local {
        try {
            return Files.readString(Path.of(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new Local();
        }
    }

    private static long u64(String text) throws Usage {
        if (text.isEmpty() || text.charAt(0) == '+' || text.charAt(0) == '-') throw new Usage();
        try {
            return Long.parseUnsignedLong(text);
        } catch (NumberFormatException e) {
            throw new Usage();
        }
    }

    private static long u32(String text) throws Usage {
        long value = u64(text);
        if (value > 0xFFFFFFFFL) throw new Usage();
        return value;
    }

    /** The plan options: a form, then {@code inspect} and {@code priority=A,B} in any order. */
    private static PlanOptions planOptions(String[] words) throws Usage {
        PlanForm form = switch (words[2]) {
            case "steps" -> PlanForm.STEPS;
            case "blob" -> PlanForm.BLOB;
            case "source" -> PlanForm.SOURCE;
            default -> throw new Usage();
        };
        boolean inspect = false;
        List<String> priority = List.of();
        for (int i = 3; i < words.length; i++) {
            if ("inspect".equals(words[i])) inspect = true;
            else if (words[i].startsWith("priority=")) priority = priorityList(words[i]);
            else throw new Usage();
        }
        return new PlanOptions(form, inspect, priority);
    }

    private static List<String> priorityList(String word) {
        return List.of(word.substring("priority=".length()).split(",", -1));
    }

    /** The start options, from word {@code from} on: {@code fifo=N}, {@code faults=N}, {@code telemetry=L}, {@code unmerged}, {@code priority=A,B}. */
    private static StartConfig startConfig(String[] words, int from) throws Usage {
        long fifo = 0;
        long faults = 0;
        TelemetryLevel telemetry = TelemetryLevel.BASIC;
        boolean unmerged = false;
        List<String> priority = List.of();
        for (int i = from; i < words.length; i++) {
            String word = words[i];
            if ("unmerged".equals(word)) {
                unmerged = true;
            } else if (word.startsWith("fifo=")) {
                fifo = u32(word.substring(5));
            } else if (word.startsWith("faults=")) {
                faults = u32(word.substring(7));
            } else if (word.startsWith("telemetry=")) {
                telemetry = switch (word.substring(10)) {
                    case "off" -> TelemetryLevel.OFF;
                    case "basic" -> TelemetryLevel.BASIC;
                    case "detailed" -> TelemetryLevel.DETAILED;
                    case "trace" -> TelemetryLevel.TRACE;
                    default -> throw new Usage();
                };
            } else if (word.startsWith("priority=")) {
                priority = priorityList(word);
            } else {
                throw new Usage();
            }
        }
        return new StartConfig(fifo, faults, telemetry, unmerged, priority);
    }

    /** The watch kinds after the target: none or {@code all}, {@code fault}, or {@code telemetry}. */
    private static EventKinds eventKinds(String[] words) throws Usage {
        if (words.length == 3 || (words.length == 4 && "all".equals(words[3]))) return EventKinds.ALL;
        if (words.length == 4 && "fault".equals(words[3])) return EventKinds.FAULT;
        if (words.length == 4 && "telemetry".equals(words[3])) return EventKinds.TELEMETRY;
        throw new Usage();
    }

    /** {@code TYPE:VALUE}, the syntax {@code daedalus-ctl} reads. Floats are not part of the suite. */
    static TypedValue parseValue(String text) throws Usage {
        int colon = text.indexOf(':');
        String name = colon < 0 ? text : text.substring(0, colon);
        String rest = colon < 0 ? "" : text.substring(colon + 1);
        if (!List.of(Types.SCALARS).contains(name)) throw new Usage();
        Object value = switch (name) {
            case "unit" -> null;
            case "bool" -> switch (rest) {
                case "true" -> Boolean.TRUE;
                case "false" -> Boolean.FALSE;
                default -> throw new Usage();
            };
            case "i8" -> (byte) signed(rest, Byte.MIN_VALUE, Byte.MAX_VALUE);
            case "i16" -> (short) signed(rest, Short.MIN_VALUE, Short.MAX_VALUE);
            case "i32" -> (int) signed(rest, Integer.MIN_VALUE, Integer.MAX_VALUE);
            case "i64", "isize" -> signed(rest, Long.MIN_VALUE, Long.MAX_VALUE);
            case "u8" -> (int) unsigned(rest, 255);
            case "u16" -> (int) unsigned(rest, 65535);
            case "u32" -> unsigned(rest, 4294967295L);
            case "u64", "usize" -> unsigned64(rest);
            case "string" -> rest;
            case "bytes" -> unhex(rest);
            default -> throw new Usage();
        };
        return new TypedValue(Types.Ty.scalar(name), value);
    }

    private static long signed(String text, long low, long high) throws Usage {
        try {
            long value = Long.parseLong(text);
            if (value < low || value > high) throw new Usage();
            return value;
        } catch (NumberFormatException e) {
            throw new Usage();
        }
    }

    private static long unsigned(String text, long high) throws Usage {
        long value = u64(text);
        if (value > high) throw new Usage();
        return value;
    }

    /** A u64 as digits: a value of at most 2^64 - 1. */
    private static BigInteger unsigned64(String text) throws Usage {
        if (text.isEmpty()) throw new Usage();
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) < '0' || text.charAt(i) > '9') throw new Usage();
        }
        BigInteger value = new BigInteger(text);
        if (value.bitLength() > 64) throw new Usage();
        return value;
    }

    /** Hex pairs, lowercase or uppercase. An empty text is no bytes. */
    static byte[] unhex(String text) throws Usage {
        if (text.length() % 2 != 0) throw new Usage();
        byte[] out = new byte[text.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int high = Character.digit(text.charAt(2 * i), 16);
            int low = Character.digit(text.charAt(2 * i + 1), 16);
            if (high < 0 || low < 0 || text.charAt(2 * i) > 127 || text.charAt(2 * i + 1) > 127) throw new Usage();
            out[i] = (byte) (high * 16 + low);
        }
        return out;
    }

    /** Codec mode: each body of the corpus file, decoded and encoded again, as a line of hex (or {@code error decode}). */
    static void recode(String kind, Path file) throws IOException {
        byte[] all = Files.readAllBytes(file);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        int at = 0;
        while (at < all.length) {
            if (all.length - at < 4) throw new IOException("a body length is cut short");
            long len = (all[at] & 0xFFL) | (all[at + 1] & 0xFFL) << 8 | (all[at + 2] & 0xFFL) << 16 | (all[at + 3] & 0xFFL) << 24;
            if (all.length - at - 4 < len) throw new IOException("a body is cut short");
            byte[] body = java.util.Arrays.copyOfRange(all, at + 4, at + 4 + (int) len);
            at += 4 + (int) len;
            String line;
            try {
                line = Show.hex(switch (kind) {
                    case "client" -> Codec.encodeClient(Codec.decodeClient(body, Set.of()), Set.of());
                    case "server" -> Codec.encodeServer(Codec.decodeServer(body, Set.of()), Set.of());
                    default -> throw new IllegalArgumentException("no message kind " + kind);
                });
            } catch (IllegalArgumentException e) {
                if (!("client".equals(kind) || "server".equals(kind))) throw e;
                line = "error decode";
            }
            out.write(line);
            out.write("\n");
        }
        out.flush();
    }
}
