package dev.daedalus.client;

import dev.daedalus.service.Model.CarryReport;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Model.PlanSummary;
import dev.daedalus.service.Model.PluginInfo;
import dev.daedalus.service.Model.StepSummary;
import dev.daedalus.service.Model.TraceEntry;
import dev.daedalus.service.Ops;
import dev.daedalus.service.Ops.EventKinds;
import dev.daedalus.service.Ops.InspectTarget;
import dev.daedalus.service.Ops.PlanOptions;
import dev.daedalus.service.Ops.StartConfig;
import dev.daedalus.service.Ops.WatchTarget;
import dev.daedalus.service.Protocol;
import dev.daedalus.service.Protocol.Diagnostic;
import dev.daedalus.service.Protocol.StopMode;
import dev.daedalus.service.Reply;
import dev.daedalus.service.TypedValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * A client of the daemon's service protocol: one connection, and one method per op.
 *
 * <p>Thread-safety: a {@code Client} may be shared by any number of threads. Requests from several
 * threads are pipelined on the one connection, and each gets its own answer. A {@link Watch} is read by
 * one thread. A connection that drops is not reopened: the calls after it fail with
 * {@link ClientException.Kind#DISCONNECTED}, and the watches end.
 *
 * <p>Sessions: {@link #inSession(String)} gives a handle on the same connection whose requests are in
 * another session. {@link #close()} closes the connection for every handle.
 *
 * <p>Failures: every call throws {@link ClientException}, checked. A refusal by the host is
 * {@link ClientException.Kind#SERVICE}, and carries the service code and the message.
 */
public final class Client implements AutoCloseable {
    /** A run's fed value: how many steps it woke, and whether the port's policy dropped a value for it. */
    public record Fed(long woke, boolean dropped) {}

    /** A run stopped: its last tick, and the values an immediate stop threw away. */
    public record Stopped(long tick, long dropped) {}

    /** A plan made from a graph: its hash, and its bytes, which a host loads with no planner. */
    public record MadePlan(long hash, byte[] bytes) {}

    private final Connection connection;
    private final String session;

    private Client(Connection connection, String session) {
        this.connection = connection;
        this.session = session;
    }

    /** Connects to the daemon and runs the hello. A host that speaks another version is refused. */
    public static Client connect(Endpoint endpoint) throws ClientException {
        return new Client(Connection.open(endpoint), Protocol.DEFAULT_SESSION);
    }

    /** The session this handle's requests are in. */
    public String session() {
        return session;
    }

    /** The same connection, with requests in session {@code name}. Refused when the name is empty or long. */
    public Client inSession(String name) throws ClientException {
        if (name.isEmpty() || name.getBytes(StandardCharsets.UTF_8).length > Protocol.SESSION_CAP) {
            throw new ClientException(ClientException.Kind.BAD_SESSION, "a session name must be 1 to 64 bytes");
        }
        return new Client(connection, name);
    }

    /** Whether the connection is still open. */
    public boolean isConnected() {
        return !connection.isClosed();
    }

    /** Closes the connection. Every call after this fails, and every watch ends. */
    @Override
    public void close() {
        connection.close();
    }

    /** The protocol version the host speaks. */
    public long ping() throws ClientException {
        if (call(new Ops.Ping()) instanceof Reply.Pong pong) return pong.version();
        throw unexpected("ping");
    }

    public PluginInfo loadPlugin(String path) throws ClientException {
        if (call(new Ops.LoadPlugin(path)) instanceof Reply.Plugin plugin) return plugin.info();
        throw unexpected("load_plugin");
    }

    /** Stores a graph document under {@code name}. Returns what the lints found in it. */
    public List<Diagnostic> putGraph(String name, String json) throws ClientException {
        if (call(new Ops.PutGraph(name, json)) instanceof Reply.Graph graph) return graph.diagnostics();
        throw unexpected("put_graph");
    }

    /** Applies a patch document to a stored graph. */
    public List<Diagnostic> patchGraph(String graph, String patch) throws ClientException {
        if (call(new Ops.PatchGraph(graph, patch)) instanceof Reply.Graph reply) return reply.diagnostics();
        throw unexpected("patch_graph");
    }

    public PlanSummary plan(String graph, PlanOptions options) throws ClientException {
        if (call(new Ops.Plan(graph, options)) instanceof Reply.Plan plan) return plan.summary();
        throw unexpected("plan");
    }

    /** Plans a stored graph into a made plan. */
    public MadePlan makePlan(String graph, StartConfig config) throws ClientException {
        if (call(new Ops.MakePlan(graph, config)) instanceof Reply.Made made) {
            return new MadePlan(made.hash(), made.bytes());
        }
        throw unexpected("make_plan");
    }

    /** Stores a made plan under {@code name}. Its bytes are read when a run starts on it. */
    public void putPlan(String name, byte[] bytes) throws ClientException {
        if (call(new Ops.PutPlan(name, bytes)) instanceof Reply.Ack) return;
        throw unexpected("put_plan");
    }

    /** Plans a stored graph and starts a run of it. */
    public Run start(String graph, StartConfig config) throws ClientException {
        if (call(new Ops.Start(graph, config)) instanceof Reply.Started started) {
            return new Run(this, started.run());
        }
        throw unexpected("start");
    }

    public Fed feed(long run, String port, TypedValue value) throws ClientException {
        if (call(new Ops.Feed(run, port, value)) instanceof Reply.Fed fed) return new Fed(fed.woke(), fed.dropped());
        throw unexpected("feed");
    }

    /** Runs {@code count} ticks. */
    public StepSummary step(long run, long count) throws ClientException {
        if (call(new Ops.Step(run, count)) instanceof Reply.Stepped stepped) return stepped.summary();
        throw unexpected("step");
    }

    /** Takes the oldest value out of an output port, or empty when the port has none. */
    public Optional<TypedValue> take(long run, String port) throws ClientException {
        if (call(new Ops.Take(run, port)) instanceof Reply.Value value) return Optional.ofNullable(value.value());
        throw unexpected("take");
    }

    /** Subscribes to the events of one run, or of every run. */
    public Watch watch(WatchTarget target, EventKinds kinds) throws ClientException {
        return connection.subscribe(target, kinds, session);
    }

    public CarryReport reload(long run, String graph) throws ClientException {
        if (call(new Ops.Reload(run, graph)) instanceof Reply.Reloaded reloaded) return reloaded.report();
        throw unexpected("reload");
    }

    public void cancel(long run) throws ClientException {
        if (call(new Ops.Cancel(run)) instanceof Reply.Ack) return;
        throw unexpected("cancel");
    }

    public Stopped stop(long run, StopMode mode) throws ClientException {
        if (call(new Ops.Stop(run, mode)) instanceof Reply.Stopped stopped) {
            return new Stopped(stopped.tick(), stopped.dropped());
        }
        throw unexpected("stop");
    }

    public Inspection inspect(InspectTarget target) throws ClientException {
        if (call(new Ops.Inspect(target)) instanceof Reply.Inspected inspected) return inspected.inspection();
        throw unexpected("inspect");
    }

    /** What the host knows of a run. */
    public Inspection.Run inspectRun(long run) throws ClientException {
        if (inspect(new InspectTarget.OfRun(run)) instanceof Inspection.Run info) return info;
        throw unexpected("inspect run");
    }

    /** A run's telemetry. */
    public Inspection.Telemetry inspectTelemetry(long run) throws ClientException {
        if (inspect(new InspectTarget.OfTelemetry(run)) instanceof Inspection.Telemetry info) return info;
        throw unexpected("inspect telemetry");
    }

    /** The plan cache's counters. */
    public Inspection.Cache cacheInfo() throws ClientException {
        if (inspect(new InspectTarget.OfCache()) instanceof Inspection.Cache cache) return cache;
        throw unexpected("inspect cache");
    }

    public void cacheClear() throws ClientException {
        if (call(new Ops.CacheClear()) instanceof Reply.Ack) return;
        throw unexpected("cache");
    }

    /** The newest {@code limit} served requests, oldest first. Zero is all the host keeps. */
    public List<TraceEntry> trace(long limit) throws ClientException {
        if (call(new Ops.Trace(limit)) instanceof Reply.Trace trace) return trace.entries();
        throw unexpected("trace");
    }

    private Reply call(Ops.Op op) throws ClientException {
        return (Reply) connection.call(session, op);
    }

    private static ClientException unexpected(String op) {
        return new ClientException(ClientException.Kind.UNEXPECTED, op + " was answered with a reply it does not take");
    }
}
