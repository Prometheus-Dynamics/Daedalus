use daedalus_daemon::service;
use std::io::Write;

fn main() {
    if let Err(err) = run() {
        let _ = writeln!(std::io::stderr(), "{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        print_usage();
        return Ok(());
    }

    let cmd = args.remove(0);
    match cmd.as_str() {
        "stdio" => {
            let engine = daedalus_engine::Engine::new(daedalus_engine::EngineConfig::default())
                .map_err(|e| e.to_string())?;
            service::run_stdio_service(engine).map_err(|e| e.to_string())
        }
        #[cfg(feature = "tcp")]
        "serve" => {
            let addr = args
                .first()
                .cloned()
                .unwrap_or_else(|| "127.0.0.1:4100".to_string());
            let engine = daedalus_engine::Engine::new(daedalus_engine::EngineConfig::default())
                .map_err(|e| e.to_string())?;
            service::run_tcp_service(engine, &addr).map_err(|e| e.to_string())
        }
        "ping" => send_simple(args, service::ServiceRequest::Ping),
        "inspect-state" => send_simple(args, service::ServiceRequest::InspectState),
        "inspect-cache" => send_simple(args, service::ServiceRequest::InspectCache),
        "clear-cache" => send_simple(args, service::ServiceRequest::ClearCache),
        "export-trace" => send_simple(args, service::ServiceRequest::ExportTrace),
        "inspect-latest" => {
            let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
            let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
            let request_id = take_flag(&mut args, "--request-id");
            let graph = take_flag(&mut args, "--graph").ok_or("--graph is required")?;
            send_request(
                &addr,
                service::ServiceEnvelope {
                    request_id,
                    session,
                    request: service::ServiceRequest::InspectLatest { graph },
                },
            )
        }
        "put-graph" => {
            let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
            let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
            let request_id = take_flag(&mut args, "--request-id");
            let name = take_flag(&mut args, "--name").ok_or("--name is required")?;
            let file = take_flag(&mut args, "--file").ok_or("--file is required")?;
            let graph: daedalus_planner::Graph =
                serde_json::from_reader(std::fs::File::open(file).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            send_request(
                &addr,
                service::ServiceEnvelope {
                    request_id,
                    session,
                    request: service::ServiceRequest::PutGraph { name, graph },
                },
            )
        }
        "put-registry" => {
            let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
            let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
            let request_id = take_flag(&mut args, "--request-id");
            let name = take_flag(&mut args, "--name").ok_or("--name is required")?;
            let file = take_flag(&mut args, "--file").ok_or("--file is required")?;
            let registry: daedalus_registry::capability::CapabilityRegistry =
                serde_json::from_reader(std::fs::File::open(file).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            send_request(
                &addr,
                service::ServiceEnvelope {
                    request_id,
                    session,
                    request: service::ServiceRequest::PutRegistry { name, registry },
                },
            )
        }
        "patch-graph" => {
            let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
            let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
            let request_id = take_flag(&mut args, "--request-id");
            let name = take_flag(&mut args, "--name").ok_or("--name is required")?;
            let file = take_flag(&mut args, "--file").ok_or("--file is required")?;
            let patch: daedalus_planner::GraphPatch =
                serde_json::from_reader(std::fs::File::open(file).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            send_request(
                &addr,
                service::ServiceEnvelope {
                    request_id,
                    session,
                    request: service::ServiceRequest::PatchGraph { name, patch },
                },
            )
        }
        "plan" | "build" | "inspect-plan" => {
            let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
            let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
            let request_id = take_flag(&mut args, "--request-id");
            let registry = take_flag(&mut args, "--registry").ok_or("--registry is required")?;
            let graph = take_flag(&mut args, "--graph").ok_or("--graph is required")?;
            let request = match cmd.as_str() {
                "plan" => service::ServiceRequest::Plan { registry, graph },
                "build" => service::ServiceRequest::Build { registry, graph },
                _ => service::ServiceRequest::InspectPlan { registry, graph },
            };
            send_request(
                &addr,
                service::ServiceEnvelope {
                    request_id,
                    session,
                    request,
                },
            )
        }
        other => Err(format!("unknown command: {other}")),
    }
}

fn send_simple(args: Vec<String>, request: service::ServiceRequest) -> Result<(), String> {
    let mut args = args;
    let addr = take_flag(&mut args, "--addr").unwrap_or_else(default_addr);
    let session = take_flag(&mut args, "--session").unwrap_or_else(default_session);
    let request_id = take_flag(&mut args, "--request-id");
    send_request(
        &addr,
        service::ServiceEnvelope {
            request_id,
            session,
            request,
        },
    )
}

fn send_request(addr: &str, request: service::ServiceEnvelope) -> Result<(), String> {
    #[cfg(feature = "tcp")]
    {
        let response = service::send_tcp_request(addr, &request).map_err(|e| e.to_string())?;
        serde_json::to_writer_pretty(std::io::stdout().lock(), &response)
            .map_err(|e| e.to_string())?;
        writeln!(std::io::stdout()).map_err(|e| e.to_string())?;
        Ok(())
    }
    #[cfg(not(feature = "tcp"))]
    {
        let _ = addr;
        let _ = request;
        Err("tcp transport is disabled for this daemon build".to_string())
    }
}

fn take_flag(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let idx = args.iter().position(|arg| arg == flag)?;
    args.remove(idx);
    if idx < args.len() {
        Some(args.remove(idx))
    } else {
        None
    }
}

fn default_addr() -> String {
    "127.0.0.1:4100".to_string()
}

fn default_session() -> String {
    "default".to_string()
}

fn print_usage() {
    let mut stderr = std::io::stderr();
    let _ = writeln!(stderr, "daedalus-daemon");
    let _ = writeln!(stderr, "  daedalus-daemon stdio");
    #[cfg(feature = "tcp")]
    let _ = writeln!(stderr, "  daedalus-daemon serve [addr]");
    let _ = writeln!(
        stderr,
        "  daedalus-daemon ping [--addr A] [--session S] [--request-id ID]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon inspect-state [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon inspect-cache [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon clear-cache [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon export-trace [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon put-graph --name N --file F [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon put-registry --name N --file F [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon patch-graph --name N --file F [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon plan --registry R --graph G [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon build --registry R --graph G [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon inspect-plan --registry R --graph G [--addr A] [--session S]"
    );
    let _ = writeln!(
        stderr,
        "  daedalus-daemon inspect-latest --graph G [--addr A] [--session S]"
    );
}
