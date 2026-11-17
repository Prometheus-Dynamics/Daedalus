package dev.daedalus.drive;

import dev.daedalus.service.Events.Event;
import dev.daedalus.service.Events.EventBody;
import dev.daedalus.service.Model.CarryReport;
import dev.daedalus.service.Model.Inspection;
import dev.daedalus.service.Model.Names;
import dev.daedalus.service.Model.PlanSummary;
import dev.daedalus.service.Model.PluginInfo;
import dev.daedalus.service.Model.StepSummary;
import dev.daedalus.service.Model.TraceEntry;
import dev.daedalus.service.Protocol.Diagnostic;
import dev.daedalus.service.Protocol.ErrorCode;
import dev.daedalus.service.TypedValue;
import java.util.List;

/**
 * The canonical text of each answer, as the drive contract states it. Every method here is the format
 * the contract names, so this driver prints the bytes the Rust reference driver prints.
 */
final class Show {
    private Show() {}

    /**
     * A string in quotes. Inside, a quote and a backslash are escaped, and so are newline, carriage
     * return and tab. A code point outside 0x20 to 0x7e is written in braces, in lowercase hex.
     */
    static String quote(String text) {
        StringBuilder out = new StringBuilder("\"");
        text.codePoints().forEach(cp -> {
            switch (cp) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (cp >= 0x20 && cp <= 0x7e) out.append((char) cp);
                    else out.append("\\u{").append(Integer.toHexString(cp)).append('}');
                }
            }
        });
        return out.append('"').toString();
    }

    /** A list: {@code [} quoted items joined by commas, {@code ]}. An empty list is {@code []}. */
    static String list(List<String> items) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) out.append(',');
            out.append(quote(items.get(i)));
        }
        return out.append(']').toString();
    }

    /** A u64 field: a negative {@code long} is the number above 2^63. */
    private static String u(long value) {
        return Long.toUnsignedString(value);
    }

    private static String bool(boolean flag) {
        return flag ? "true" : "false";
    }

    /**
     * A value as {@code TYPE:VALUE}. A scalar of the suite's kinds is written; a float or a compound
     * value is {@code other}, since the suite has none.
     */
    static String value(TypedValue typed) {
        if (!"scalar".equals(typed.ty().kind)) return "other";
        String name = typed.ty().scalar;
        Object value = typed.value();
        return switch (name) {
            case "unit" -> "unit:";
            case "bool" -> "bool:" + bool((Boolean) value);
            case "i8", "i16", "i32", "i64", "isize", "u8", "u16", "u32", "u64", "usize" -> name + ":" + value;
            case "string" -> "string:" + value;
            case "bytes" -> "bytes:" + hex((byte[]) value);
            default -> "other";
        };
    }

    /** Lowercase hex, two digits a byte. */
    static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) out.append(String.format("%02x", b & 0xFF));
        return out.toString();
    }

    private static String diagnostics(List<Diagnostic> found) {
        return list(found.stream().map(item -> item.severity().label() + "|" + item.code()).toList());
    }

    static String error(ErrorCode code) {
        return "error " + code.label();
    }

    static String pong(long version) {
        return "pong version=" + version;
    }

    static String session(String name) {
        return "session name=" + quote(name);
    }

    static String plugin(PluginInfo info) {
        return "plugin name=" + quote(info.name()) + " generation=" + info.generation()
            + " nodes=" + list(info.nodes()) + " backends=" + list(info.backends());
    }

    static String graph(String name, List<Diagnostic> found) {
        return "graph name=" + quote(name) + " diagnostics=" + diagnostics(found);
    }

    static String plan(PlanSummary summary) {
        String explain = summary.explain() == null ? "none" : quote(summary.explain());
        return "plan graph=" + quote(summary.graph()) + " hash=" + u(summary.hash()) + " form=" + summary.form().label()
            + " nodes=" + summary.nodes() + " edges=" + summary.edges() + " regions=" + summary.regions()
            + " steps=" + summary.steps() + " cached=" + bool(summary.cached())
            + " diagnostics=" + diagnostics(summary.diagnostics()) + " explain=" + explain;
    }

    static String blob(String name, int len) {
        return "blob name=" + quote(name) + " bytes=" + len;
    }

    static String made(long hash, int len) {
        return "made hash=" + u(hash) + " bytes=" + len;
    }

    static String started(long run) {
        return "started run=" + run;
    }

    static String fed(long woke, boolean dropped) {
        return "fed woke=" + woke + " dropped=" + bool(dropped);
    }

    static String stepped(StepSummary summary) {
        return "stepped ticks=" + summary.ticks() + " tick=" + u(summary.tick()) + " step_calls=" + u(summary.stepCalls())
            + " faults=" + u(summary.faults()) + " skipped=" + u(summary.skipped()) + " idle=" + bool(summary.idle());
    }

    static String taken(TypedValue value) {
        return value == null ? "value none" : "value " + value(value);
    }

    static String watching(long id) {
        return "watching " + id;
    }

    static String unwatched(long id) {
        return "unwatched " + id;
    }

    static String streamEnded(long id) {
        return "stream " + id + " ended";
    }

    private static String names(Names found) {
        return list(found.names()) + "/" + found.total();
    }

    static String reloaded(CarryReport report) {
        return "reloaded carried=" + names(report.carried()) + " fresh=" + names(report.fresh())
            + " added=" + names(report.added()) + " removed=" + names(report.removed())
            + " removed_ports=" + names(report.removedPorts()) + " dropped_values=" + report.droppedValues();
    }

    static String ack() {
        return "ack";
    }

    static String stopped(long tick, long dropped) {
        return "stopped tick=" + u(tick) + " dropped=" + dropped;
    }

    static String cache(Inspection.Cache info) {
        return "inspect cache entries=" + info.entries() + " hits=" + u(info.hits()) + " misses=" + u(info.misses());
    }

    static String inspection(Inspection found) {
        return switch (found) {
            case Inspection.Run info -> {
                List<String> ports = info.ports().stream()
                    .map(port -> port.name() + "|" + (port.input() ? "input" : "output") + "|" + port.typeKey())
                    .toList();
                yield "inspect run run=" + u(info.run()) + " session=" + quote(info.session()) + " graph=" + quote(info.graph())
                    + " state=" + info.state().label() + " tick=" + u(info.tick()) + " faults=" + u(info.faults())
                    + " ports=" + list(ports) + " nodes=" + list(info.nodes());
            }
            case Inspection.Graph graph -> "inspect graph name=" + quote(graph.name()) + " json=" + quote(graph.json());
            case Inspection.Cache info -> cache(info);
            case Inspection.Telemetry info -> telemetry(info);
            case Inspection.Host info -> {
                List<String> runs = info.runs().stream()
                    .map(run -> u(run.run()) + "|" + run.session() + "|" + run.graph() + "|" + run.state().label())
                    .toList();
                yield "inspect host sessions=" + list(info.sessions()) + " graphs=" + list(info.graphs())
                    + " runs=" + list(runs) + " plugins=" + list(info.plugins());
            }
        };
    }

    private static String telemetry(Inspection.Telemetry info) {
        // Timings and byte counts are measured, so they are left out of the transcript.
        List<String> rows = info.rows().stream()
            .map(row -> row.step() + "|" + String.join("+", row.nodes()) + "|" + row.calls() + "|" + row.failures()
                + "|" + bool(row.backingOff()) + "|" + bool(row.disabled()) + "|" + bool(row.autoOff()))
            .toList();
        return "inspect telemetry run=" + u(info.run()) + " level=" + info.level().label() + " tick=" + u(info.tick())
            + " step_calls=" + u(info.stepCalls()) + " faults=" + u(info.faults()) + " skipped=" + u(info.skipped())
            + " rows=" + list(rows);
    }

    static String trace(List<TraceEntry> entries) {
        List<String> items = entries.stream()
            .map(entry -> u(entry.seq()) + "|" + entry.op().label() + "|" + entry.target() + "|"
                + (entry.error() == null ? "none" : entry.error().label()))
            .toList();
        return "trace entries=" + list(items);
    }

    /** The line for an event read from watch {@code id}. */
    static String event(long id, Event event) {
        EventBody body = event.body();
        return switch (body) {
            case dev.daedalus.service.Events.Fault fault -> "event " + id + " fault run=" + u(fault.run())
                + " tick=" + fault.tick() + " step=" + fault.step()
                + " node=" + (fault.node() == null ? "none" : quote(fault.node()))
                + " covers=" + list(fault.covers()) + " consecutive=" + fault.consecutive()
                + " reason=" + quote(fault.reason()) + " reason_cut=" + bool(fault.reasonCut())
                + " detail=" + quote(fault.detail()) + " detail_cut=" + bool(fault.detailCut())
                + " code=" + fault.code() + " own_code=" + bool(fault.ownCode());
            case dev.daedalus.service.Events.Telemetry t -> "event " + id + " telemetry run=" + u(t.run()) + " tick=" + u(t.tick())
                + " step_calls=" + u(t.stepCalls()) + " faults=" + u(t.faults()) + " skipped=" + u(t.skipped());
            case dev.daedalus.service.Events.Lagged lagged -> "event " + id + " lagged dropped=" + u(lagged.dropped());
            case dev.daedalus.service.Events.Ended ended -> "event " + id + " ended run=" + u(ended.run())
                + " reason=" + ended.reason().label();
            case dev.daedalus.service.Events.Unknown unknown -> "event " + id + " unknown kind=" + unknown.kind();
        };
    }
}
