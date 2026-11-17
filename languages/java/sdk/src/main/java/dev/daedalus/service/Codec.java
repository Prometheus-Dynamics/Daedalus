package dev.daedalus.service;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Events.EventBody;
import dev.daedalus.service.Messages.ClientMsg;
import dev.daedalus.service.Messages.ServerMsg;
import dev.daedalus.service.Model.CarryReport;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Model.Names;
import dev.daedalus.service.Model.PlanSummary;
import dev.daedalus.service.Model.PluginInfo;
import dev.daedalus.service.Model.RunBrief;
import dev.daedalus.service.Model.StepSummary;
import dev.daedalus.service.Model.TraceEntry;
import dev.daedalus.service.Ops.Op;
import dev.daedalus.service.Protocol.Capabilities;
import dev.daedalus.service.Protocol.Diagnostic;
import dev.daedalus.service.Protocol.EndReason;
import dev.daedalus.service.Protocol.ErrorCode;
import dev.daedalus.service.Protocol.OpKind;
import dev.daedalus.service.Protocol.Outcome;
import dev.daedalus.service.Protocol.PlanForm;
import dev.daedalus.service.Protocol.RunState;
import dev.daedalus.service.Protocol.ServiceError;
import dev.daedalus.service.Protocol.ServiceHello;
import dev.daedalus.service.Protocol.Severity;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.Protocol.TelemetryLevel;
import dev.daedalus.wire.Bytes;
import dev.daedalus.wire.Types;
import dev.daedalus.wire.Values;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * The service codec: every client and server message, read and written as {@code core/wire} writes
 * them. A third copy of the Rust codec, held to the shared corpus by a test (see {@code drive}).
 *
 * <p>The rules a reader follows are the Rust ones. A record is read with its length: the fields this
 * side knows are read, and the rest of the record is skipped. A field added at the end of a record is
 * read only when {@code more()} says there are bytes left. Anything after a whole message is ignored,
 * since a newer side may put fields there. An op this side does not have is refused as
 * {@link IllegalArgumentException} (the server answers it {@code unsupported}); an event kind this side
 * does not have is {@link Events.Unknown}; an error code this side does not have is
 * {@link ErrorCode#OTHER}.
 *
 * <p>Every bad input throws {@link IllegalArgumentException}, which includes truncated input.
 */
public final class Codec {
    private Codec() {}

    // ---- messages ----------------------------------------------------------------------------

    /** Writes a client's message. {@code codecs} are the opaque type keys the server accepts. */
    public static byte[] encodeClient(ClientMsg msg, Set<String> codecs) {
        Bytes.Writer out = new Bytes.Writer();
        switch (msg) {
            case ClientMsg.Hello hello -> {
                out.u8(Protocol.CLIENT_HELLO);
                writeHello(out, hello.hello());
            }
            case ClientMsg.Request request -> {
                if (request.session().getBytes(StandardCharsets.UTF_8).length > Protocol.SESSION_CAP) {
                    throw new IllegalArgumentException("a session name over " + Protocol.SESSION_CAP + " bytes");
                }
                out.u8(Protocol.REQUEST);
                out.u64(request.id());
                out.str(request.session());
                writeOp(out, request.op(), codecs);
            }
        }
        return out.toByteArray();
    }

    /** Reads a client's message. */
    public static ClientMsg decodeClient(byte[] body, Set<String> codecs) {
        Bytes.Reader in = new Bytes.Reader(body);
        int kind = in.u8();
        return switch (kind) {
            case Protocol.CLIENT_HELLO -> new ClientMsg.Hello(readHello(in));
            case Protocol.REQUEST -> {
                long id = in.u64();
                String session = in.str();
                if (session.getBytes(StandardCharsets.UTF_8).length > Protocol.SESSION_CAP) {
                    throw new IllegalArgumentException("a session name over " + Protocol.SESSION_CAP + " bytes");
                }
                yield new ClientMsg.Request(id, session, readOp(in, codecs));
            }
            default -> throw new IllegalArgumentException("unknown message kind " + kind);
        };
    }

    /** The id of a request whose body could not be decoded, when the envelope got that far. */
    public static long requestId(byte[] body) {
        Bytes.Reader in = new Bytes.Reader(body);
        if (in.u8() != Protocol.REQUEST) throw new IllegalArgumentException("not a request");
        return in.u64();
    }

    /** Writes a server's message. */
    public static byte[] encodeServer(ServerMsg msg, Set<String> codecs) {
        Bytes.Writer out = new Bytes.Writer();
        switch (msg) {
            case ServerMsg.Hello hello -> {
                out.u8(Protocol.SERVER_HELLO);
                writeHello(out, hello.hello());
            }
            case ServerMsg.Response response -> {
                out.u8(Protocol.RESPONSE);
                out.u64(response.id());
                if (response.outcome() instanceof ServiceError error) {
                    out.u8(1);
                    writeError(out, error);
                } else {
                    out.u8(0);
                    writeReply(out, (dev.daedalus.service.Reply) response.outcome(), codecs);
                }
            }
            case ServerMsg.EventMsg event -> {
                out.u8(Protocol.EVENT);
                out.u64(event.event().sub());
                writeEvent(out, event.event().body());
            }
        }
        return out.toByteArray();
    }

    /** Reads a server's message. */
    public static ServerMsg decodeServer(byte[] body, Set<String> codecs) {
        Bytes.Reader in = new Bytes.Reader(body);
        int kind = in.u8();
        return switch (kind) {
            case Protocol.SERVER_HELLO -> new ServerMsg.Hello(readHello(in));
            case Protocol.RESPONSE -> {
                long id = in.u64();
                Outcome outcome = flag(in, "result flag") ? readError(in) : readReply(in, codecs);
                yield new ServerMsg.Response(id, outcome);
            }
            case Protocol.EVENT -> {
                long sub = in.u64();
                yield new ServerMsg.EventMsg(new Event(sub, readEvent(in)));
            }
            default -> throw new IllegalArgumentException("unknown message kind " + kind);
        };
    }

    // ---- hello ------------------------------------------------------------------------------

    private static void writeHello(Bytes.Writer out, ServiceHello hello) {
        out.u32(hello.version());
        out.u64(hello.layout());
        out.u32(hello.packetCap());
        out.len(hello.capabilities().ops().size());
        for (OpKind op : hello.capabilities().ops()) out.u8(op.code());
        strs(out, hello.capabilities().codecs());
        out.u32(hello.capabilities().maxRuns());
        strs(out, hello.knows());
        strs(out, hello.needs());
    }

    private static ServiceHello readHello(Bytes.Reader in) {
        long version = in.u32();
        long layout = in.u64();
        long packetCap = in.u32();
        int count = in.count();
        List<OpKind> ops = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            // A newer server serves ops this side doesn't have. They are no use here, and no reason to refuse.
            OpKind op = OpKind.named(in.u8());
            if (op != OpKind.OTHER) ops.add(op);
        }
        List<String> codecs = strList(in);
        long maxRuns = in.u32();
        List<String> knows = strList(in);
        List<String> needs = strList(in);
        return new ServiceHello(version, layout, packetCap, new Capabilities(ops, codecs, maxRuns), knows, needs);
    }

    // ---- requests ---------------------------------------------------------------------------

    private static void writeOp(Bytes.Writer out, Op op, Set<String> codecs) {
        out.u8(op.kind().code());
        switch (op) {
            case Ops.LoadPlugin o -> out.str(o.path());
            case Ops.PutGraph o -> { out.str(o.name()); out.str(o.json()); }
            case Ops.PatchGraph o -> { out.str(o.graph()); out.str(o.patch()); }
            case Ops.Plan o -> { out.str(o.graph()); planOptions(out, o.options()); }
            case Ops.MakePlan o -> { out.str(o.graph()); startConfig(out, o.options()); }
            case Ops.PutPlan o -> { out.str(o.name()); out.bytes(o.bytes()); }
            case Ops.Start o -> { out.str(o.graph()); startConfig(out, o.config()); }
            case Ops.Feed o -> {
                out.u64(o.run());
                out.str(o.port());
                writeTyped(out, o.value(), codecs);
            }
            case Ops.Step o -> { out.u64(o.run()); out.u32(o.count()); }
            case Ops.Take o -> { out.u64(o.run()); out.str(o.port()); }
            case Ops.Watch o -> {
                if (o.target() instanceof Ops.WatchTarget.OneRun one) {
                    out.u8(1);
                    out.u64(one.run());
                } else {
                    out.u8(0);
                }
                out.u8(o.kinds().bits());
            }
            case Ops.Unwatch o -> out.u64(o.sub());
            case Ops.Reload o -> { out.u64(o.run()); out.str(o.graph()); }
            case Ops.Cancel o -> out.u64(o.run());
            case Ops.Stop o -> { out.u64(o.run()); out.u8(o.mode().code()); }
            case Ops.Inspect o -> writeInspectTarget(out, o.target());
            case Ops.CacheClear o -> out.u8(0);
            case Ops.Trace o -> out.u32(o.limit());
            case Ops.Ping o -> { }
        }
    }

    private static void writeInspectTarget(Bytes.Writer out, Ops.InspectTarget target) {
        switch (target) {
            case Ops.InspectTarget.OfRun t -> { out.u8(0); out.u64(t.run()); }
            case Ops.InspectTarget.OfGraph t -> { out.u8(1); out.str(t.name()); }
            case Ops.InspectTarget.OfCache t -> out.u8(2);
            case Ops.InspectTarget.OfTelemetry t -> { out.u8(3); out.u64(t.run()); }
            case Ops.InspectTarget.OfHost t -> out.u8(4);
        }
    }

    private static Op readOp(Bytes.Reader in, Set<String> codecs) {
        int code = in.u8();
        OpKind kind = OpKind.named(code);
        if (kind == OpKind.OTHER) throw new IllegalArgumentException("unknown op " + code);
        return switch (kind) {
            case LOAD_PLUGIN -> new Ops.LoadPlugin(in.str());
            case PUT_GRAPH -> new Ops.PutGraph(in.str(), in.str());
            case PATCH_GRAPH -> new Ops.PatchGraph(in.str(), in.str());
            case PLAN -> new Ops.Plan(in.str(), readPlanOptions(in));
            case START -> new Ops.Start(in.str(), readStartConfig(in));
            case FEED -> new Ops.Feed(in.u64(), in.str(), readTyped(in, codecs));
            case STEP -> new Ops.Step(in.u64(), in.u32());
            case TAKE -> new Ops.Take(in.u64(), in.str());
            case WATCH -> {
                int tag = in.u8();
                Ops.WatchTarget target = switch (tag) {
                    case 0 -> new Ops.WatchTarget.Everything();
                    case 1 -> new Ops.WatchTarget.OneRun(in.u64());
                    default -> throw new IllegalArgumentException("bad watch target " + tag);
                };
                int bits = in.u8();
                if ((bits & ~3) != 0) throw new IllegalArgumentException("bad event kinds " + bits);
                yield new Ops.Watch(target, new Ops.EventKinds(bits));
            }
            case UNWATCH -> new Ops.Unwatch(in.u64());
            case RELOAD -> new Ops.Reload(in.u64(), in.str());
            case CANCEL -> new Ops.Cancel(in.u64());
            case STOP -> new Ops.Stop(in.u64(), stopMode(in));
            case INSPECT -> new Ops.Inspect(readInspectTarget(in));
            case CACHE -> {
                int action = in.u8();
                if (action != 0) throw new IllegalArgumentException("bad cache action " + action);
                yield new Ops.CacheClear();
            }
            case TRACE -> new Ops.Trace(in.u32());
            case PING -> new Ops.Ping();
            case MAKE_PLAN -> new Ops.MakePlan(in.str(), readStartConfig(in));
            case PUT_PLAN -> new Ops.PutPlan(in.str(), in.bytes());
            default -> throw new IllegalArgumentException("unknown op " + code);
        };
    }

    private static Ops.InspectTarget readInspectTarget(Bytes.Reader in) {
        int tag = in.u8();
        return switch (tag) {
            case 0 -> new Ops.InspectTarget.OfRun(in.u64());
            case 1 -> new Ops.InspectTarget.OfGraph(in.str());
            case 2 -> new Ops.InspectTarget.OfCache();
            case 3 -> new Ops.InspectTarget.OfTelemetry(in.u64());
            case 4 -> new Ops.InspectTarget.OfHost();
            default -> throw new IllegalArgumentException("bad inspect target " + tag);
        };
    }

    private static void planOptions(Bytes.Writer out, Ops.PlanOptions options) {
        out.record(w -> {
            w.u8(options.form().code());
            w.bool(options.inspect());
            strs(w, options.priority());
        });
    }

    private static Ops.PlanOptions readPlanOptions(Bytes.Reader in) {
        return in.record(r -> new Ops.PlanOptions(byCode(PlanForm.values(), r.u8(), "plan form"), flag(r, "inspect flag"),
            strList(r)));
    }

    private static void startConfig(Bytes.Writer out, Ops.StartConfig config) {
        out.record(w -> {
            w.u32(config.fifoCapacity());
            w.u32(config.faultReports());
            w.u8(config.telemetry().code());
            w.bool(config.unmerged());
            strs(w, config.priority());
        });
    }

    private static Ops.StartConfig readStartConfig(Bytes.Reader in) {
        return in.record(r -> new Ops.StartConfig(r.u32(), r.u32(),
            byCode(TelemetryLevel.values(), r.u8(), "telemetry level"), flag(r, "unmerged flag"), strList(r)));
    }

    private static StopMode stopMode(Bytes.Reader in) {
        return byCode(StopMode.values(), in.u8(), "stop mode");
    }

    // ---- replies and errors -----------------------------------------------------------------

    private static void writeReply(Bytes.Writer out, dev.daedalus.service.Reply reply, Set<String> codecs) {
        switch (reply) {
            case dev.daedalus.service.Reply.Pong r -> { out.u8(0); out.u32(r.version()); }
            case dev.daedalus.service.Reply.Ack r -> out.u8(1);
            case dev.daedalus.service.Reply.Plugin r -> {
                out.u8(2);
                out.str(r.info().name());
                out.u32(r.info().generation());
                strs(out, r.info().nodes());
                strs(out, r.info().backends());
            }
            case dev.daedalus.service.Reply.Graph r -> {
                out.u8(3);
                out.str(r.name());
                diagnostics(out, r.diagnostics());
            }
            case dev.daedalus.service.Reply.Plan r -> {
                PlanSummary p = r.summary();
                out.u8(4);
                out.str(p.graph());
                out.u64(p.hash());
                out.u8(p.form().code());
                out.u32(p.nodes());
                out.u32(p.edges());
                out.u32(p.regions());
                out.u32(p.steps());
                out.bool(p.cached());
                diagnostics(out, p.diagnostics());
                optStr(out, p.explain());
            }
            case dev.daedalus.service.Reply.Started r -> { out.u8(5); out.u64(r.run()); }
            case dev.daedalus.service.Reply.Fed r -> { out.u8(6); out.u32(r.woke()); out.bool(r.dropped()); }
            case dev.daedalus.service.Reply.Stepped r -> {
                StepSummary s = r.summary();
                out.u8(7);
                out.u32(s.ticks());
                out.u64(s.tick());
                out.u64(s.stepCalls());
                out.u64(s.faults());
                out.u64(s.skipped());
                out.bool(s.idle());
            }
            case dev.daedalus.service.Reply.Value r -> {
                out.u8(8);
                if (r.value() == null) out.u8(0);
                else {
                    out.u8(1);
                    writeTyped(out, r.value(), codecs);
                }
            }
            case dev.daedalus.service.Reply.Watching r -> { out.u8(9); out.u64(r.sub()); }
            case dev.daedalus.service.Reply.Reloaded r -> {
                CarryReport c = r.report();
                out.u8(10);
                names(out, c.carried());
                names(out, c.fresh());
                names(out, c.added());
                names(out, c.removed());
                names(out, c.removedPorts());
                out.u32(c.droppedValues());
            }
            case dev.daedalus.service.Reply.Stopped r -> {
                out.u8(11);
                out.u64(r.run());
                out.u64(r.tick());
                out.u32(r.dropped());
            }
            case dev.daedalus.service.Reply.Inspected r -> {
                out.u8(12);
                writeInspection(out, r.inspection());
            }
            case dev.daedalus.service.Reply.Trace r -> {
                out.u8(13);
                list(out, r.entries(), Codec::writeTrace);
            }
            case dev.daedalus.service.Reply.Made r -> { out.u8(14); out.u64(r.hash()); out.bytes(r.bytes()); }
        }
    }

    private static dev.daedalus.service.Reply readReply(Bytes.Reader in, Set<String> codecs) {
        int tag = in.u8();
        return switch (tag) {
            case 0 -> new dev.daedalus.service.Reply.Pong(in.u32());
            case 1 -> new dev.daedalus.service.Reply.Ack();
            case 2 -> new dev.daedalus.service.Reply.Plugin(new PluginInfo(in.str(), in.u32(), strList(in), strList(in)));
            case 3 -> new dev.daedalus.service.Reply.Graph(in.str(), diagnosticList(in));
            case 4 -> new dev.daedalus.service.Reply.Plan(new PlanSummary(in.str(), in.u64(),
                byCode(PlanForm.values(), in.u8(), "plan form"), in.u32(), in.u32(), in.u32(), in.u32(),
                flag(in, "cached flag"), diagnosticList(in), optStr(in)));
            case 5 -> new dev.daedalus.service.Reply.Started(in.u64());
            case 6 -> new dev.daedalus.service.Reply.Fed(in.u32(), flag(in, "dropped flag"));
            case 7 -> new dev.daedalus.service.Reply.Stepped(new StepSummary(in.u32(), in.u64(), in.u64(), in.u64(),
                in.u64(), flag(in, "idle flag")));
            case 8 -> new dev.daedalus.service.Reply.Value(flag(in, "value flag") ? readTyped(in, codecs) : null);
            case 9 -> new dev.daedalus.service.Reply.Watching(in.u64());
            case 10 -> new dev.daedalus.service.Reply.Reloaded(new CarryReport(names(in), names(in), names(in),
                names(in), names(in), in.u32()));
            case 11 -> new dev.daedalus.service.Reply.Stopped(in.u64(), in.u64(), in.u32());
            case 12 -> new dev.daedalus.service.Reply.Inspected(readInspection(in));
            case 13 -> new dev.daedalus.service.Reply.Trace(list(in, Codec::readTrace));
            case 14 -> new dev.daedalus.service.Reply.Made(in.u64(), in.bytes());
            default -> throw new IllegalArgumentException("bad reply " + tag);
        };
    }

    private static void writeError(Bytes.Writer out, ServiceError error) {
        out.u16(error.code().code());
        out.str(error.message());
        diagnostics(out, error.diagnostics());
    }

    private static ServiceError readError(Bytes.Reader in) {
        ErrorCode code = ErrorCode.fromCode(in.u16());
        return new ServiceError(code, in.str(), diagnosticList(in));
    }

    private static void diagnostics(Bytes.Writer out, List<Diagnostic> items) {
        list(out, items, (w, d) -> w.record(r -> {
            r.u8(d.severity().code());
            r.str(d.code());
            r.str(d.message());
            optStr(r, d.node());
            optStr(r, d.port());
        }));
    }

    private static List<Diagnostic> diagnosticList(Bytes.Reader in) {
        return list(in, r -> r.record(f -> new Diagnostic(byCode(Severity.values(), f.u8(), "severity"),
            f.str(), f.str(), optStr(f), optStr(f))));
    }

    private static void names(Bytes.Writer out, Names names) {
        out.record(w -> {
            strs(w, names.names());
            w.u32(names.total());
        });
    }

    private static Names names(Bytes.Reader in) {
        return in.record(r -> new Names(strList(r), r.u32()));
    }

    private static void writeTrace(Bytes.Writer out, TraceEntry entry) {
        out.record(w -> {
            w.u64(entry.seq());
            w.u64(entry.id());
            w.str(entry.session());
            w.u8(entry.op().code());
            w.str(entry.target());
            w.u16(entry.error() == null ? 0 : entry.error().code());
        });
    }

    private static TraceEntry readTrace(Bytes.Reader in) {
        return in.record(r -> {
            long seq = r.u64();
            long id = r.u64();
            String session = r.str();
            OpKind op = OpKind.named(r.u8());
            String target = r.str();
            int error = r.u16();
            return new TraceEntry(seq, id, session, op, target, error == 0 ? null : ErrorCode.fromCode(error));
        });
    }

    private static void writeInspection(Bytes.Writer out, Inspection inspection) {
        switch (inspection) {
            case Inspection.Run run -> {
                out.u8(0);
                out.record(w -> {
                    w.u64(run.run());
                    w.str(run.session());
                    w.str(run.graph());
                    w.u8(run.state().code());
                    w.u64(run.tick());
                    w.u64(run.faults());
                    list(w, run.ports(), (x, port) -> x.record(p -> {
                        p.str(port.name());
                        p.bool(port.input());
                        p.str(port.typeKey());
                    }));
                    strs(w, run.nodes());
                });
            }
            case Inspection.Graph graph -> {
                out.u8(1);
                out.str(graph.name());
                out.str(graph.json());
            }
            case Inspection.Cache cache -> {
                out.u8(2);
                out.u32(cache.entries());
                out.u64(cache.hits());
                out.u64(cache.misses());
            }
            case Inspection.Telemetry telemetry -> {
                out.u8(3);
                out.u64(telemetry.run());
                out.u8(telemetry.level().code());
                out.u64(telemetry.tick());
                out.u64(telemetry.stepCalls());
                out.u64(telemetry.faults());
                out.u64(telemetry.skipped());
                list(out, telemetry.rows(), (w, row) -> w.record(r -> {
                    r.u32(row.step());
                    strs(r, row.nodes());
                    r.u32(row.calls());
                    r.u32(row.failures());
                    r.bool(row.backingOff());
                    r.bool(row.disabled());
                    r.bool(row.autoOff());
                    r.u32(row.timedRuns());
                    r.u64(row.timeNs());
                    r.u64(row.bytes());
                }));
            }
            case Inspection.Host host -> {
                out.u8(4);
                strs(out, host.sessions());
                strs(out, host.graphs());
                list(out, host.runs(), (w, run) -> w.record(r -> {
                    r.u64(run.run());
                    r.str(run.session());
                    r.str(run.graph());
                    r.u8(run.state().code());
                }));
                strs(out, host.plugins());
            }
        }
    }

    private static Inspection readInspection(Bytes.Reader in) {
        int tag = in.u8();
        return switch (tag) {
            case 0 -> in.record(r -> new Inspection.Run(r.u64(), r.str(), r.str(),
                byCode(RunState.values(), r.u8(), "run state"), r.u64(), r.u64(),
                list(r, p -> p.record(f -> new Inspection.Port(f.str(), flag(f, "port direction"), f.str()))),
                strList(r)));
            case 1 -> new Inspection.Graph(in.str(), in.str());
            case 2 -> new Inspection.Cache(in.u32(), in.u64(), in.u64());
            case 3 -> new Inspection.Telemetry(in.u64(), byCode(TelemetryLevel.values(), in.u8(), "telemetry level"),
                in.u64(), in.u64(), in.u64(), in.u64(), list(in, r -> r.record(Codec::readRow)));
            case 4 -> new Inspection.Host(strList(in), strList(in), list(in, r -> r.record(f -> new RunBrief(
                f.u64(), f.str(), f.str(), byCode(RunState.values(), f.u8(), "run state")))), strList(in));
            default -> throw new IllegalArgumentException("bad inspection " + tag);
        };
    }

    /** A telemetry row. The timing fields came later, all together, so a row without them is read as zero. */
    private static Inspection.Row readRow(Bytes.Reader r) {
        long step = r.u32();
        List<String> nodes = strList(r);
        long calls = r.u32();
        long failures = r.u32();
        boolean backingOff = flag(r, "backing off");
        boolean disabled = flag(r, "disabled");
        boolean autoOff = flag(r, "auto off");
        long timedRuns = 0, timeNs = 0, bytes = 0;
        if (r.more()) {
            timedRuns = r.u32();
            timeNs = r.u64();
            bytes = r.u64();
        }
        return new Inspection.Row(step, nodes, calls, failures, backingOff, disabled, autoOff, timedRuns, timeNs, bytes);
    }

    // ---- events -----------------------------------------------------------------------------

    private static void writeEvent(Bytes.Writer out, EventBody body) {
        switch (body) {
            case Events.Fault f -> {
                out.u8(0);
                out.u64(f.run());
                out.u32(f.tick());
                out.u32(f.step());
                optStr(out, f.node());
                strs(out, f.covers());
                out.u32(f.consecutive());
                out.str(f.reason());
                out.bool(f.reasonCut());
                out.str(f.detail());
                out.bool(f.detailCut());
                out.u32(f.code());
                out.bool(f.ownCode());
            }
            case Events.Telemetry t -> {
                out.u8(1);
                out.u64(t.run());
                out.u64(t.tick());
                out.u64(t.stepCalls());
                out.u64(t.faults());
                out.u64(t.skipped());
            }
            case Events.Lagged l -> { out.u8(2); out.u64(l.dropped()); }
            case Events.Ended e -> { out.u8(3); out.u64(e.run()); out.u8(e.reason().code()); }
            case Events.Unknown u -> out.u8(u.kind());
        }
    }

    private static EventBody readEvent(Bytes.Reader in) {
        int kind = in.u8();
        return switch (kind) {
            case 0 -> {
                long run = in.u64();
                long tick = in.u32();
                long step = in.u32();
                String node = optStr(in);
                List<String> covers = strList(in);
                long consecutive = in.u32();
                String reason = in.str();
                boolean reasonCut = flag(in, "reason cut");
                String detail = in.str();
                boolean detailCut = flag(in, "detail cut");
                // Added together, after the first version: both are there, or neither is.
                long code = 0;
                boolean ownCode = false;
                if (in.more()) {
                    code = in.u32();
                    ownCode = flag(in, "own code");
                }
                yield new Events.Fault(run, tick, step, node, covers, consecutive, reason, reasonCut, detail,
                    detailCut, code, ownCode);
            }
            case 1 -> new Events.Telemetry(in.u64(), in.u64(), in.u64(), in.u64(), in.u64());
            case 2 -> new Events.Lagged(in.u64());
            case 3 -> new Events.Ended(in.u64(), byCode(EndReason.values(), in.u8(), "end reason"));
            default -> new Events.Unknown(kind);
        };
    }

    // ---- typed values -----------------------------------------------------------------------

    private static void writeTyped(Bytes.Writer out, TypedValue typed, Set<String> codecs) {
        Types.encode(typed.ty(), out, 0);
        Values.encode(typed.ty(), typed.value(), new Values.Schema(codecs), out, 0);
    }

    private static TypedValue readTyped(Bytes.Reader in, Set<String> codecs) {
        Types.Ty ty = Types.decode(in, 0);
        return new TypedValue(ty, Values.decode(ty, in, new Values.Schema(codecs), 0));
    }

    // ---- helpers ----------------------------------------------------------------------------

    private static boolean flag(Bytes.Reader in, String what) {
        int tag = in.u8();
        if (tag > 1) throw new IllegalArgumentException("bad " + what + " " + tag);
        return tag == 1;
    }

    private static void optStr(Bytes.Writer out, String text) {
        if (text == null) out.u8(0);
        else {
            out.u8(1);
            out.str(text);
        }
    }

    private static String optStr(Bytes.Reader in) {
        return flag(in, "optional flag") ? in.str() : null;
    }

    private static void strs(Bytes.Writer out, List<String> items) {
        list(out, items, Bytes.Writer::str);
    }

    private static List<String> strList(Bytes.Reader in) {
        return list(in, Bytes.Reader::str);
    }

    private static <T> void list(Bytes.Writer out, List<T> items, BiConsumer<Bytes.Writer, T> each) {
        out.len(items.size());
        for (T item : items) each.accept(out, item);
    }

    private static <T> List<T> list(Bytes.Reader in, Function<Bytes.Reader, T> each) {
        int count = in.count();
        List<T> items = new ArrayList<>(count);
        for (int i = 0; i < count; i++) items.add(each.apply(in));
        return items;
    }

    private static <E extends Enum<E> & Protocol.Coded> E byCode(E[] all, int code, String what) {
        for (E item : all) if (item.code() == code) return item;
        throw new IllegalArgumentException("bad " + what + " " + code);
    }
}
