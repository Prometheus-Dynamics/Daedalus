//! Made plans on the listener, and the ops that need the planner. Without the planner, those ops
//! are refused as unsupported and left out of the hello; with it, a made plan stored by put_plan
//! runs the same as the graph it was made from.

mod listen_support;

use std::sync::Arc;
use std::thread;

use daedalus_host::listen::{ListenOptions, ServiceHost};
#[cfg(not(feature = "planner"))]
use daedalus_wire::service::OpKind;
use daedalus_wire::service::{
    ClientMsg, DEFAULT_SESSION, ErrorCode, Op, Reply, Request, ServerMsg, ServiceError,
    ServiceHello, StartConfig,
};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};
use listen_support::{graph, host};

/// One stdio session on `host`: each call sends a request and reads its answer.
struct Session {
    writes: std::io::PipeWriter,
    reads: std::io::PipeReader,
    buf: Vec<u8>,
    next: u64,
}

fn session(host: &Arc<ServiceHost>) -> Session {
    let (host_reads, client_writes) = std::io::pipe().unwrap();
    let (client_reads, host_writes) = std::io::pipe().unwrap();
    let host = Arc::clone(host);
    thread::spawn(move || host.serve_stream(host_reads, host_writes));
    Session {
        writes: client_writes,
        reads: client_reads,
        buf: Vec::new(),
        next: 0,
    }
}

impl Session {
    fn send(&mut self, message: ClientMsg) -> ServerMsg {
        let body = message.encode(&Schema::new()).unwrap();
        write_packet(&mut self.writes, &body, DEFAULT_CAP).unwrap();
        read_packet(&mut self.reads, DEFAULT_CAP, &mut self.buf).unwrap();
        ServerMsg::decode(&self.buf, &Schema::new()).unwrap()
    }

    fn hello(&mut self) -> ServiceHello {
        match self.send(ClientMsg::Hello(ServiceHello::client())) {
            ServerMsg::Hello(hello) => hello,
            other => panic!("expected a hello, got {other:?}"),
        }
    }

    fn ask(&mut self, op: Op) -> Result<Reply, ServiceError> {
        self.next += 1;
        let id = self.next;
        match self.send(ClientMsg::Request(Request::new(id, op))) {
            ServerMsg::Response(response) => {
                assert_eq!(response.id, id);
                response.result
            }
            other => panic!("expected a response, got {other:?}"),
        }
    }
}

#[test]
fn a_stored_plan_must_be_a_made_plan() {
    let host = host(ListenOptions::default());
    let refused = host
        .put_plan(DEFAULT_SESSION, "g", b"not a plan")
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::InvalidRequest);
    let refused = host
        .put_plan(DEFAULT_SESSION, "", b"not a plan")
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::InvalidRequest);
}

#[test]
fn a_run_of_a_name_nothing_stores_is_refused() {
    let host = host(ListenOptions::default());
    let mut client = session(&host);
    client.hello();
    let refused = client
        .ask(Op::Start {
            graph: "missing".to_string(),
            config: StartConfig::default(),
        })
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::UnknownGraph);
}

#[cfg(not(feature = "planner"))]
#[test]
fn a_host_without_the_planner_refuses_the_ops_that_plan_and_leaves_them_out() {
    let host = host(ListenOptions::default());
    let mut client = session(&host);
    let hello = client.hello();
    for op in [
        OpKind::PutGraph,
        OpKind::PatchGraph,
        OpKind::Plan,
        OpKind::MakePlan,
        OpKind::Cache,
    ] {
        assert!(!hello.capabilities.ops.contains(&op), "{op:?} is offered");
    }
    assert!(hello.capabilities.ops.contains(&OpKind::PutPlan));
    let unsupported = |result: Result<Reply, ServiceError>| {
        assert_eq!(result.unwrap_err().code, ErrorCode::Unsupported);
    };
    unsupported(client.ask(Op::PutGraph {
        name: "g".to_string(),
        json: graph("t.double"),
    }));
    unsupported(client.ask(Op::MakePlan {
        graph: "g".to_string(),
        options: StartConfig::default(),
    }));
    unsupported(client.ask(Op::Cache {
        action: daedalus_wire::service::CacheAction::Clear,
    }));
}

#[cfg(feature = "planner")]
#[test]
fn a_made_plan_starts_like_the_graph_it_was_made_from() {
    let host = host(ListenOptions::default());
    let mut client = session(&host);
    client.hello();
    client
        .ask(Op::PutGraph {
            name: "g".to_string(),
            json: graph("t.double"),
        })
        .unwrap();
    let Reply::Made { hash, bytes } = client
        .ask(Op::MakePlan {
            graph: "g".to_string(),
            options: StartConfig::default(),
        })
        .unwrap()
    else {
        panic!("the made plan");
    };
    assert_eq!(hash, daedalus_types::fnv1a64(&bytes));
    host.put_plan(DEFAULT_SESSION, "made", &bytes).unwrap();
    let started = client
        .ask(Op::Start {
            graph: "made".to_string(),
            config: StartConfig::default(),
        })
        .unwrap();
    assert!(matches!(started, Reply::Started { .. }), "{started:?}");
    // A graph stored under the same name replaces the plan.
    client
        .ask(Op::PutGraph {
            name: "made".to_string(),
            json: graph("t.double"),
        })
        .unwrap();
    assert!(
        host.put_plan(DEFAULT_SESSION, "made", &bytes).is_ok(),
        "a plan can replace a graph too"
    );
}

#[cfg(feature = "planner")]
#[test]
fn a_made_plan_refuses_a_run_that_asks_to_plan_again() {
    let host = host(ListenOptions::default());
    let mut client = session(&host);
    client.hello();
    client
        .ask(Op::PutGraph {
            name: "g".to_string(),
            json: graph("t.double"),
        })
        .unwrap();
    let Reply::Made { bytes, .. } = client
        .ask(Op::MakePlan {
            graph: "g".to_string(),
            options: StartConfig::default(),
        })
        .unwrap()
    else {
        panic!("the made plan");
    };
    host.put_plan(DEFAULT_SESSION, "made", &bytes).unwrap();
    let unmerged = StartConfig {
        unmerged: true,
        ..StartConfig::default()
    };
    let refused = client
        .ask(Op::Start {
            graph: "made".to_string(),
            config: unmerged,
        })
        .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Unsupported);
}
