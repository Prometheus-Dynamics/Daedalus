"""The Python driver for the drive conformance suite: `daedalus.client` run under a scenario.

The suite runs this program as `python3 driver.py ENDPOINT`, with the endpoint as the last
argument. It reads the whole of standard input, one command per line, and writes one answer line per
command, in the forms the driver contract in `testing/crates/drive-conformance/src/lib.rs` gives.
The rules there are complete. The Rust reference driver (`drive-reference`) is the model for every
answer, and `tests/drive.rs` checks this program against its golden transcripts.

The codec mode (`--recode`) is not here: this client's codec is the Rust one, whose corpus is
checked by the Rust client's own tests. Errors are answered as the contract says: a refusal by the
daemon with its code, a usage error for a line that is not a command, and `local` for anything else.
"""

import os
import re
import sys
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "sdk"))

from daedalus import wire  # noqa: E402
from daedalus.client import (  # noqa: E402
    EVENT_ALL,
    EVENT_FAULT,
    EVENT_TELEMETRY,
    Client,
    DaedalusError,
    ServiceError,
    StartConfig,
)

_SIGNED = {"i8": 8, "i16": 16, "i32": 32, "i64": 64, "isize": 64}
_UNSIGNED = {"u8": 8, "u16": 16, "u32": 32, "u64": 64, "usize": 64}
_DECIMAL = re.compile(r"^[0-9]+$")
_SIGNED_DECIMAL = re.compile(r"^-?[0-9]+$")
_HEX = re.compile(r"^(?:[0-9a-fA-F]{2})*$")


class Usage(Exception):
    """A line that is not a command, or names a watch or blob the scenario never made."""


class Local(Exception):
    """A failure of this program that is not the daemon's: a file it cannot read."""


# ---- the canonical text of an answer ----------------------------------------------------------


def quote(text):
    """A string in quotes. Inside: `\\"`, `\\\\`, `\\n`, `\\r`, `\\t`. Other code points outside
    U+0020 to U+007E are `\\u{H}`, H being the code point in lowercase hex."""
    out = ['"']
    for ch in text:
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif " " <= ch <= "~":
            out.append(ch)
        else:
            out.append("\\u{%x}" % ord(ch))
    out.append('"')
    return "".join(out)


def items(values):
    """A list: quoted items joined by commas, in brackets. An empty list is `[]`."""
    return "[" + ",".join(quote(value) for value in values) + "]"


def boolean(flag):
    return "true" if flag else "false"


def hexed(data):
    return "".join("%02x" % byte for byte in data)


def unhex(text):
    if not _HEX.match(text):
        return None
    return bytes.fromhex(text)


def value_text(typed):
    """A taken value as `TYPE:VALUE`. Floats and compound values are `other`."""
    ty = typed.ty
    if not (isinstance(ty, tuple) and ty[0] == "scalar"):
        return "other"
    name = ty[1]
    value = typed.value
    if name == "unit":
        return "unit:"
    if name == "bool":
        return "bool:" + boolean(value)
    if name in _SIGNED or name in _UNSIGNED:
        return "%s:%d" % (name, value)
    if name == "string":
        return "string:" + value
    if name == "bytes":
        return "bytes:" + hexed(value)
    return "other"


def parse_value(text):
    """`TYPE:VALUE`, as the suite writes a value. Floats are not part of the suite."""
    name, _, rest = text.partition(":")
    if name == "unit":
        return ("scalar", "unit"), None
    if name == "bool":
        if rest not in ("true", "false"):
            raise Usage(text)
        return ("scalar", "bool"), rest == "true"
    if name in _SIGNED or name in _UNSIGNED:
        pattern = _SIGNED_DECIMAL if name in _SIGNED else _DECIMAL
        if not pattern.match(rest):
            raise Usage(text)
        number = int(rest)
        bits = _SIGNED.get(name) or _UNSIGNED[name]
        if name in _SIGNED:
            low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
        else:
            low, high = 0, 2**bits - 1
        if not low <= number <= high:
            raise Usage(text)
        return ("scalar", name), number
    if name == "string":
        return ("scalar", "string"), rest
    if name == "bytes":
        data = unhex(rest)
        if data is None:
            raise Usage(text)
        return ("scalar", "bytes"), data
    raise Usage(text)


def number(text):
    """A whole number as the suite writes one: decimal, no sign."""
    if not _DECIMAL.match(text):
        raise Usage(text)
    return int(text)


def explain_text(text):
    """The planner's explanation, quoted, or `none` when the plan had none."""
    return "none" if text is None else quote(text)


# ---- answers --------------------------------------------------------------------------------------


def diagnostics(found):
    return items(["%s|%s" % (lint["severity"], lint["code"]) for lint in found])


def event_line(watch_id, event):
    kind = event.get("event")
    if kind == "fault":
        node = quote(event["node"]) if event.get("node") is not None else "none"
        return (
            "event %d fault run=%d tick=%d step=%d node=%s covers=%s consecutive=%d "
            "reason=%s reason_cut=%s detail=%s detail_cut=%s code=%d own_code=%s"
        ) % (
            watch_id,
            event["run"],
            event["tick"],
            event["step"],
            node,
            items(event["covers"]),
            event["consecutive"],
            quote(event["reason"]),
            boolean(event["reason_cut"]),
            quote(event["detail"]),
            boolean(event["detail_cut"]),
            event["code"],
            boolean(event["own_code"]),
        )
    if kind == "telemetry":
        return "event %d telemetry run=%d tick=%d step_calls=%d faults=%d skipped=%d" % (
            watch_id,
            event["run"],
            event["tick"],
            event["step_calls"],
            event["faults"],
            event["skipped"],
        )
    if kind == "lagged":
        return "event %d lagged dropped=%d" % (watch_id, event["dropped"])
    if kind == "ended":
        return "event %d ended run=%d reason=%s" % (watch_id, event["run"], event["reason"])
    return "event %d unknown kind=%d" % (watch_id, event["kind"])


def error_line(error):
    if isinstance(error, ServiceError):
        return "error %s" % error.code
    if isinstance(error, (Usage, ValueError, TypeError, wire.WireError)):
        return "error usage"
    return "error local"


# ---- the driver -----------------------------------------------------------------------------------


class Driver:
    """The state a scenario builds up: its sessions, watches and blobs."""

    def __init__(self, root):
        self.root = root
        self.current = root
        self.watches = {}
        self.pending = {}
        self.blobs = {}

    def answer(self, line):
        try:
            return self.run(line)
        except Local:
            return "error local"
        except (Usage, DaedalusError, ValueError, TypeError, wire.WireError) as error:
            return error_line(error)

    def run(self, line):
        words = line.split(" ")
        command = words[0]
        args = words[1:]
        if command == "ping" and not args:
            return "pong version=%d" % self.current.ping()
        if command == "session" and len(args) <= 1:
            name = args[0] if args else "default"
            self.current = self.root.session(name)
            return "session name=%s" % quote(name)
        if command == "load_plugin" and len(args) == 1:
            record = self.current.load_plugin(args[0])
            return "plugin name=%s generation=%d nodes=%s backends=%s" % (
                quote(record["name"]),
                record["generation"],
                items(record["nodes"]),
                items(record["backends"]),
            )
        if command in ("put_graph", "patch_graph") and len(args) == 2:
            name, file_name = args
            try:
                with open(file_name, encoding="utf-8") as handle:
                    text = handle.read()
            except (OSError, UnicodeDecodeError):
                raise Local(file_name) from None
            if command == "put_graph":
                found = self.current.put_graph(name, text)
            else:
                found = self.current.patch_graph(name, text)
            return "graph name=%s diagnostics=%s" % (quote(name), diagnostics(found))
        if command == "plan" and len(args) >= 2:
            return self.plan(args[0], args[1], args[2:])
        if command == "blob" and len(args) == 2:
            data = unhex(args[1])
            if data is None:
                raise Usage(line)
            self.blobs[args[0]] = data
            return "blob name=%s bytes=%d" % (quote(args[0]), len(data))
        if command == "make_plan" and len(args) >= 2:
            plan_hash, data = self.current.make_plan(args[0], start_config(args[2:]))
            self.blobs[args[1]] = data
            return "made hash=%d bytes=%d" % (plan_hash, len(data))
        if command == "put_plan" and len(args) == 2:
            if args[1] not in self.blobs:
                raise Usage(line)
            self.current.put_plan(args[0], self.blobs[args[1]])
            return "ack"
        if command == "start" and len(args) >= 1:
            run = self.current.start(args[0], start_config(args[1:]))
            return "started run=%d" % run.id
        if command == "feed" and len(args) >= 3:
            run_text, port = args[0], args[1]
            typed = line.split(" ", 3)[3] if line.count(" ") >= 3 else None
            if typed is None:
                raise Usage(line)
            ty, value = parse_value(typed)
            fed = self.current.feed(number(run_text), port, value, ty)
            return "fed woke=%d dropped=%s" % (fed["woke"], boolean(fed["dropped"]))
        if command == "step" and len(args) == 2:
            done = self.current.step(number(args[0]), number(args[1]))
            return (
                "stepped ticks=%d tick=%d step_calls=%d faults=%d skipped=%d idle=%s"
                % (
                    done["ticks"],
                    done["tick"],
                    done["step_calls"],
                    done["faults"],
                    done["skipped"],
                    boolean(done["idle"]),
                )
            )
        if command == "take" and len(args) == 2:
            taken = self.current.take(number(args[0]), args[1])
            if taken is None:
                return "value none"
            return "value " + value_text(taken)
        if command == "watch" and len(args) >= 2:
            return self.open_watch(args)
        if command == "next" and len(args) == 1:
            return self.next_event(number(args[0]))
        if command == "unwatch" and len(args) == 1:
            return self.unwatch(number(args[0]))
        if command == "reload" and len(args) == 2:
            report = self.current.reload(number(args[0]), args[1])
            return "reloaded " + " ".join(
                "%s=%s/%d" % (key, items(report[key]["names"]), report[key]["total"])
                for key in ("carried", "fresh", "added", "removed", "removed_ports")
            ) + " dropped_values=%d" % report["dropped_values"]
        if command == "cancel" and len(args) == 1:
            self.current.cancel(number(args[0]))
            return "ack"
        if command == "stop" and len(args) == 2 and args[1] in ("drain", "immediate"):
            stopped = self.current.stop(number(args[0]), args[1])
            return "stopped tick=%d dropped=%d" % (stopped["tick"], stopped["dropped"])
        if command == "inspect":
            return self.inspect(args)
        if command == "cache" and args == ["clear"]:
            self.current.cache_clear()
            return "ack"
        if command == "trace" and len(args) == 1:
            return self.trace(number(args[0]))
        raise Usage(line)

    def plan(self, graph, form, rest):
        options = {"form": form, "inspect": False, "priority": ()}
        if form not in ("steps", "blob", "source"):
            raise Usage(form)
        for word in rest:
            if word == "inspect":
                options["inspect"] = True
            elif word.startswith("priority="):
                options["priority"] = word[len("priority="):].split(",")
            else:
                raise Usage(word)
        found = self.current.plan(
            graph, form, inspect=options["inspect"], priority=options["priority"]
        )
        return (
            "plan graph=%s hash=%d form=%s nodes=%d edges=%d regions=%d steps=%d cached=%s "
            "diagnostics=%s explain=%s"
        ) % (
            quote(found["graph"]),
            int(found["hash"], 16),
            found["form"],
            found["nodes"],
            found["edges"],
            found["regions"],
            found["steps"],
            boolean(found["cached"]),
            diagnostics(found["diagnostics"]),
            explain_text(found["explain"]),
        )

    def open_watch(self, args):
        watch_id = number(args[0])
        target = args[1]
        run = None if target == "all" else number(target)
        kinds = args[2:]
        if kinds in ([], ["all"]):
            bits = EVENT_ALL
        elif kinds == ["fault"]:
            bits = EVENT_FAULT
        elif kinds == ["telemetry"]:
            bits = EVENT_TELEMETRY
        else:
            raise Usage(" ".join(args))
        self.watches[watch_id] = self.current.watch(run, bits)
        self.pending[watch_id] = deque()
        return "watching %d" % watch_id

    def next_event(self, watch_id):
        watch = self.watches.get(watch_id)
        if watch is None:
            raise Usage(str(watch_id))
        queued = self.pending[watch_id]
        if queued:
            return event_line(watch_id, queued.popleft())
        if watch.closed:
            return "stream %d ended" % watch_id
        event = watch.next_event()
        if event is None:
            return "stream %d ended" % watch_id
        return event_line(watch_id, event)

    def unwatch(self, watch_id):
        watch = self.watches.get(watch_id)
        if watch is None:
            raise Usage(str(watch_id))
        if not watch.closed:
            # Events already queued stay readable after the unwatch, as the contract says.
            self.pending[watch_id].extend(watch.drain())
            watch.close()
        return "unwatched %d" % watch_id

    def inspect(self, args):
        if args[:1] == ["run"] and len(args) == 2:
            found = self.current.inspect_run(number(args[1]))
            ports = [
                "%s|%s|%s" % (port["name"], "input" if port["input"] else "output", port["type"])
                for port in found["ports"]
            ]
            return (
                "inspect run run=%d session=%s graph=%s state=%s tick=%d faults=%d ports=%s nodes=%s"
            ) % (
                found["run"],
                quote(found["session"]),
                quote(found["graph"]),
                found["state"],
                found["tick"],
                found["faults"],
                items(ports),
                items(found["nodes"]),
            )
        if args[:1] == ["telemetry"] and len(args) == 2:
            found = self.current.inspect_telemetry(number(args[1]))
            rows = [
                "%d|%s|%d|%d|%s|%s|%s"
                % (
                    row["step"],
                    "+".join(row["nodes"]),
                    row["calls"],
                    row["failures"],
                    boolean(row["backing_off"]),
                    boolean(row["disabled"]),
                    boolean(row["auto_off"]),
                )
                for row in found["rows"]
            ]
            return (
                "inspect telemetry run=%d level=%s tick=%d step_calls=%d faults=%d skipped=%d rows=%s"
            ) % (
                found["run"],
                found["level"],
                found["tick"],
                found["step_calls"],
                found["faults"],
                found["skipped"],
                items(rows),
            )
        if args[:1] == ["graph"] and len(args) == 2:
            text = self.current.graph_text(args[1])
            return "inspect graph name=%s json=%s" % (quote(args[1]), quote(text))
        if args == ["cache"]:
            found = self.current.inspect_cache()
            return "inspect cache entries=%d hits=%d misses=%d" % (
                found["entries"],
                found["hits"],
                found["misses"],
            )
        if args == ["host"]:
            found = self.current.inspect_host()
            runs = ["%d|%s|%s|%s" % (run["run"], run["session"], run["graph"], run["state"]) for run in found["runs"]]
            return "inspect host sessions=%s graphs=%s runs=%s plugins=%s" % (
                items(found["sessions"]),
                items(found["graphs"]),
                items(runs),
                items(found["plugins"]),
            )
        raise Usage(" ".join(args))

    def trace(self, limit):
        found = self.current.trace(limit)
        entries = [
            "%d|%s|%s|%s" % (entry["seq"], entry["op"], entry["target"], entry["error"] or "none")
            for entry in found["entries"]
        ]
        return "trace entries=%s" % items(entries)

    def close(self):
        for watch in self.watches.values():
            watch.close()
        self.root.close()


def start_config(words):
    """The start options, as the suite writes them: `unmerged`, `fifo=N`, `faults=N`,
    `telemetry=LEVEL` and `priority=A,B`, in any order."""
    fields = {}
    for word in words:
        if word == "unmerged":
            fields["unmerged"] = True
        elif word.startswith("fifo="):
            fields["fifo"] = number(word[len("fifo="):])
        elif word.startswith("faults="):
            fields["faults"] = number(word[len("faults="):])
        elif word.startswith("telemetry="):
            fields["telemetry"] = word[len("telemetry="):]
        elif word.startswith("priority="):
            fields["priority"] = tuple(word[len("priority="):].split(","))
        else:
            raise Usage(word)
    try:
        return StartConfig(**fields)
    except ValueError:
        raise Usage(words) from None


def main(argv):
    if len(argv) != 1:
        sys.stderr.write("usage: driver.py ENDPOINT\n")
        return 2
    try:
        root = Client.connect(argv[0])
    except DaedalusError as error:
        sys.stderr.write("cannot connect: %s\n" % error)
        return 1
    driver = Driver(root)
    text = sys.stdin.read()
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    out = sys.stdout
    try:
        for line in lines:
            if line.endswith("\r"):
                line = line[:-1]
            out.write(driver.answer(line) + "\n")
            out.flush()
    finally:
        driver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
