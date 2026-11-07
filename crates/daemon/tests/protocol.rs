//! The daemon's wire protocol, end to end through the binary: newline-delimited JSON requests
//! over stdin/stdout (always), and the same protocol over TCP with the `serve` and CLI
//! subcommands (behind the `tcp` feature).

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use daedalus_planner::Graph;
use serde_json::{Value, json};

fn daemon() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daedalus-daemon"))
}

#[test]
fn stdio_answers_ping_put_graph_inspect_and_errors_in_lockstep() {
    let mut child = daemon()
        .arg("stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn daemon");
    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout"));

    let graph = serde_json::to_value(Graph::default()).expect("graph json");
    let requests = [
        json!({"op": "ping", "request_id": "1"}),
        json!({"op": "put_graph", "request_id": "2", "session": "s", "name": "g", "graph": graph}),
        json!({"op": "inspect_state", "session": "s"}),
        json!({"op": "get_graph", "session": "s", "name": "absent"}),
        json!({"op": "no_such_op"}),
    ];
    let mut responses = Vec::new();
    for request in requests {
        writeln!(stdin, "{request}").expect("write request");
        stdin.flush().expect("flush");
        let mut line = String::new();
        stdout.read_line(&mut line).expect("read response");
        responses.push(serde_json::from_str::<Value>(&line).expect("response json"));
    }
    // A line that is not JSON gets an `invalid_request` error and the daemon keeps going.
    writeln!(stdin, "not json").expect("write");
    stdin.flush().expect("flush");
    let mut line = String::new();
    stdout.read_line(&mut line).expect("read");
    responses.push(serde_json::from_str(&line).expect("response json"));
    drop(stdin);
    assert!(
        child.wait().expect("wait").success(),
        "daemon exits cleanly at EOF"
    );

    assert_eq!(
        responses[0],
        json!({"request_id": "1", "session": "default", "kind": "pong", "protocol_version": 1})
    );
    assert_eq!(
        responses[1],
        json!({"request_id": "2", "session": "s", "kind": "ack"})
    );
    assert_eq!(responses[2]["kind"], "state");
    // Any session-scoped request creates its namespace, the ping's `default` included.
    assert_eq!(responses[2]["sessions"], json!(["default", "s"]));
    assert_eq!(responses[2]["graphs"], json!(["g"]));
    assert_eq!(responses[3]["kind"], "error");
    assert_eq!(responses[3]["code"], "missing_graph");
    assert_eq!(responses[4]["kind"], "error");
    assert_eq!(responses[4]["code"], "invalid_request");
    assert_eq!(responses[5]["kind"], "error");
    assert_eq!(responses[5]["code"], "invalid_request");
}

/// Start `serve` on port 0 and read back the address it bound. Picking a free port first and
/// passing it in races with other processes taking that port before the daemon binds it.
#[cfg(feature = "tcp")]
fn spawn_server() -> (std::process::Child, String) {
    let mut server = daemon()
        .args(["serve", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn server");
    let mut line = String::new();
    BufReader::new(server.stdout.take().expect("server stdout"))
        .read_line(&mut line)
        .expect("read listening line");
    let Some(addr) = line.trim().strip_prefix("listening ") else {
        let _ = server.kill();
        let _ = server.wait();
        panic!("daemon did not report its address: {line:?}");
    };
    (server, addr.to_string())
}

#[cfg(feature = "tcp")]
#[test]
fn tcp_serves_the_protocol_to_the_cli() {
    let (mut server, addr) = spawn_server();
    let result = std::panic::catch_unwind(|| {
        let ping = daemon()
            .args(["ping", "--addr", &addr])
            .output()
            .expect("ping");
        assert!(
            ping.status.success(),
            "{}",
            String::from_utf8_lossy(&ping.stderr)
        );
        let body: Value = serde_json::from_slice(&ping.stdout).expect("ping json");
        assert_eq!(body["kind"], "pong");
        assert_eq!(body["protocol_version"], 1);

        let file = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("daemon-graph.json");
        let graph = serde_json::to_string(&Graph::default()).expect("graph json");
        std::fs::write(&file, graph).expect("write graph");
        let put = daemon()
            .args(["put-graph", "--name", "g", "--file"])
            .arg(&file)
            .args(["--addr", &addr])
            .output()
            .expect("put-graph");
        assert!(
            put.status.success(),
            "{}",
            String::from_utf8_lossy(&put.stderr)
        );

        let state = daemon()
            .args(["inspect-state", "--addr", &addr])
            .output()
            .expect("state");
        let body: Value = serde_json::from_slice(&state.stdout).expect("state json");
        assert_eq!(body["graphs"], json!(["g"]));
    });
    server.kill().expect("stop server");
    server.wait().expect("reap server");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
