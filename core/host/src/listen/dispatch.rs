//! Each op, mapped onto the planner and the loaded hosts. A dispatch that panics is answered
//! with an error, and the host goes on.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use daedalus_wire::service::{
    CacheAction, CarryReport, EndReason, ErrorCode, EventBody, EventKinds, HostInfo, InspectTarget,
    Inspection, Names, Op, OpRef, PluginInfo, Reply, RequestRef, Response, RunId, ServiceError,
    StartConfig, StepSummary, StopMode, TelemetryEvent, TypedValue,
};

use super::runs::RunEntry;
use super::session::Session;
use super::values::FeedRefusal;
use super::{Engine, ServiceHost, lock};
use crate::report::BoundedNames;
use crate::{HostError, LoadOptions, LoadedHost, ReloadError, ReloadReport};

type Answer = Result<Reply, ServiceError>;

fn refuse(code: ErrorCode, message: impl Into<String>) -> ServiceError {
    ServiceError::new(code, message)
}

fn names(list: &BoundedNames) -> Names {
    Names {
        names: list.names().to_vec(),
        total: u32::try_from(list.total()).unwrap_or(u32::MAX),
    }
}

fn carry(report: &ReloadReport) -> CarryReport {
    CarryReport {
        carried: names(&report.carried),
        fresh: names(&report.fresh),
        added: names(&report.added),
        removed: names(&report.removed),
        removed_ports: names(&report.removed_ports),
        dropped_values: report.dropped_values,
    }
}

/// A port's type key, for a refusal's message.
fn port_key(host: &crate::Host<'static>, port: &str, dir: crate::Dir) -> String {
    host.port_infos()
        .find(|info| info.name == port && info.dir == dir)
        .map_or_else(String::new, |info| String::from(info.type_key))
}

fn port_error(error: &HostError) -> ServiceError {
    let code = match error {
        HostError::UnknownPort { .. }
        | HostError::NotAnInput { .. }
        | HostError::NotAnOutput { .. } => ErrorCode::UnknownPort,
        HostError::Full { .. } => ErrorCode::Busy,
        HostError::Run(_) | HostError::EdgeFull { .. } => ErrorCode::InvalidRequest,
        _ => ErrorCode::ValueType,
    };
    refuse(code, crate::render_error(error))
}

impl ServiceHost {
    /// Serves one request. The answer always carries the request's id, whatever happens.
    pub(crate) fn dispatch(&self, session: &Arc<Session>, request: RequestRef<'_>) -> Response {
        let served = catch_unwind(AssertUnwindSafe(|| self.serve(session, &request)));
        let result = served.unwrap_or_else(|panic| {
            let what = daedalus_abi::panic_message(&*panic);
            Err(refuse(
                ErrorCode::InvalidRequest,
                std::format!("{} panicked in the host: {what}", request.op.kind().name()),
            ))
        });
        let code = result.as_ref().err().map(|error| error.code);
        lock(&self.trace).record_ref(&request, code);
        Response {
            id: request.id,
            result,
        }
    }

    fn run(&self, run: RunId) -> Result<Arc<RunEntry>, ServiceError> {
        lock(&self.runs)
            .get(&run)
            .cloned()
            .ok_or_else(|| refuse(ErrorCode::UnknownRun, std::format!("no run {run}")))
    }

    fn serve(&self, session: &Arc<Session>, request: &RequestRef<'_>) -> Answer {
        if self.is_stopping() {
            return Err(refuse(ErrorCode::Stopped, "the host is shutting down"));
        }
        let space = request.session;
        let op = match &request.op {
            OpRef::Feed { run, port, value } => return self.feed(&self.run(*run)?, port, value),
            OpRef::Step { run, count } => return self.step(&self.run(*run)?, *count),
            OpRef::Take { run, port } => return self.take(&self.run(*run)?, port),
            OpRef::Owned(op) => op,
        };
        match op {
            Op::Ping => Ok(Reply::Pong {
                version: daedalus_wire::service::SERVICE_VERSION,
            }),
            Op::LoadPlugin { path } => self.load_plugin(path).map(Reply::Plugin),
            Op::PutGraph { name, json } => self.put_graph(space, name, json),
            Op::PatchGraph { graph, patch } => self.patch_graph(space, graph, patch),
            Op::Plan { graph, options } => self.plan(space, graph, options),
            Op::MakePlan { graph, options } => self.make_plan(space, graph, options),
            Op::PutPlan { name, bytes } => self.put_plan(space, name, bytes).map(|()| Reply::Ack),
            Op::Start { graph, config } => self.start(space, graph, config),
            Op::Feed { run, port, value } => self.feed(&self.run(*run)?, port, value),
            Op::Step { run, count } => self.step(&self.run(*run)?, *count),
            Op::Take { run, port } => self.take(&self.run(*run)?, port),
            Op::Watch { target, kinds } => {
                if let daedalus_wire::service::WatchTarget::Run(run) = target {
                    self.run(*run)?;
                }
                let sub = lock(&self.watchers).add(session, *target, *kinds);
                Ok(Reply::Watching { sub })
            }
            Op::Unwatch { sub } => match lock(&self.watchers).remove(session, *sub) {
                true => Ok(Reply::Ack),
                false => Err(refuse(
                    ErrorCode::InvalidRequest,
                    std::format!("this connection has no watch {sub}"),
                )),
            },
            Op::Reload { run, graph } => self.reload(space, &self.run(*run)?, graph),
            Op::Cancel { run } => {
                let entry = self.run(*run)?;
                entry.cancel();
                lock(&self.watchers).end_run(entry.id, EndReason::Cancelled);
                Ok(Reply::Ack)
            }
            Op::Stop { run, mode } => self.stop(&self.run(*run)?, *mode),
            Op::Inspect { target } => self.inspect(space, target).map(Reply::Inspection),
            Op::Cache {
                action: CacheAction::Clear,
            } => self.clear_cache(),
            Op::Trace { limit } => Ok(Reply::Trace(lock(&self.trace).recent(*limit))),
        }
    }

    /// Loads a plugin through the host's loader, as the `load_plugin` op does. The embedder
    /// calls this for the plugins it loads at start.
    pub fn load_plugin(&self, path: &str) -> Result<PluginInfo, ServiceError> {
        let mut engine = lock(&self.engine);
        let Engine {
            registry,
            backends,
            plugins,
            loaded,
            ..
        } = &mut *engine;
        let Some(loader) = plugins else {
            return Err(refuse(ErrorCode::Unsupported, "this host loads no plugins"));
        };
        let info = loader
            .load(path, registry, backends)
            .map_err(|reason| refuse(ErrorCode::PluginError, reason))?;
        if !loaded.contains(&info.name) {
            loaded.push(info.name.clone());
        }
        Ok(info)
    }

    fn load_options(config: &StartConfig) -> LoadOptions {
        let defaults = LoadOptions::default();
        LoadOptions {
            fifo_capacity: match config.fifo_capacity {
                0 => defaults.fifo_capacity,
                capacity => capacity,
            },
            fault_reports: match config.fault_reports {
                0 => defaults.fault_reports,
                reports => reports as usize,
            },
            overhead_window: 0,
            fault_detail_bytes: defaults.fault_detail_bytes,
        }
    }

    fn start(&self, session: &str, name: &str, config: &StartConfig) -> Answer {
        let limit = self.options().max_runs as usize;
        if limit > 0 && lock(&self.runs).len() >= limit {
            return Err(refuse(
                ErrorCode::Busy,
                std::format!("the host holds {limit} runs at once"),
            ));
        }
        let bound = lock(&self.engine).bound(session, name, config)?;
        let host = LoadedHost::load(bound, Self::load_options(config))
            .map_err(|error| ServiceError::planning(error.to_string(), Vec::new()))?;
        let run = self.next_run();
        let entry = Arc::new(RunEntry::new(run, session, name, host, config.clone()));
        lock(&self.runs).insert(run, entry);
        Ok(Reply::Started { run })
    }

    fn feed(&self, entry: &Arc<RunEntry>, port: &str, value: &TypedValue) -> Answer {
        entry.live()?;
        let mut run = lock(&entry.run);
        let host = run.host.host();
        // Looked up, not copied: the codec belongs to the service host, so nothing is held from
        // the run's host past this line. A refusal looks the key up again for its message.
        let codec = self.port_codec(host, port, crate::Dir::Input, entry.id)?;
        if codec.ty != value.ty {
            let key = port_key(host, port, crate::Dir::Input);
            return Err(refuse(
                ErrorCode::ValueType,
                std::format!(
                    "port `{port}` carries `{key}`, which travels as {}, not as {}",
                    codec.ty.name(),
                    value.ty.name()
                ),
            ));
        }
        match (codec.feed)(host, port, &value.value) {
            Ok(outcome) => Ok(Reply::Fed {
                woke: outcome.woke,
                dropped: outcome.dropped.is_some(),
            }),
            Err(FeedRefusal::Shape) => {
                let key = port_key(host, port, crate::Dir::Input);
                Err(refuse(
                    ErrorCode::ValueType,
                    std::format!("the value is not a `{key}`"),
                ))
            }
            Err(FeedRefusal::Host(error)) => Err(port_error(&error)),
        }
    }

    /// The codec of a run's port, by the port's type key. Lookup only: a port the run doesn't
    /// have, or a key with no codec, is refused, and nothing is added anywhere.
    fn port_codec(
        &self,
        host: &crate::Host<'static>,
        port: &str,
        dir: crate::Dir,
        run: RunId,
    ) -> Result<&super::WireCodec, ServiceError> {
        let info = host
            .port_infos()
            .find(|info| info.name == port && info.dir == dir)
            .ok_or_else(|| {
                let which = match dir {
                    crate::Dir::Input => "input",
                    crate::Dir::Output => "output",
                };
                refuse(
                    ErrorCode::UnknownPort,
                    std::format!("run {run} has no {which} port `{port}`"),
                )
            })?;
        self.codecs.get(info.type_key).ok_or_else(|| {
            refuse(
                ErrorCode::Unsupported,
                std::format!(
                    "port `{port}` carries `{}`, which has no wire codec",
                    info.type_key
                ),
            )
        })
    }

    fn step(&self, entry: &Arc<RunEntry>, count: u32) -> Answer {
        entry.live()?;
        let mut summary = StepSummary::default();
        for _ in 0..count {
            let ticked = entry.tick(self.now_ns())?;
            summary.ticks += 1;
            summary.step_calls += u64::from(ticked.report.step_calls);
            summary.faults += u64::from(ticked.report.faults);
            summary.skipped += u64::from(ticked.report.skipped);
            summary.idle = ticked.report.idle;
            if !ticked.faults.is_empty() {
                let mut watchers = lock(&self.watchers);
                for fault in ticked.faults {
                    watchers.emit(entry.id, EventKinds::FAULT, &EventBody::Fault(fault));
                }
            }
        }
        summary.tick = lock(&entry.run).tick;
        if summary.ticks > 0 {
            let event = EventBody::Telemetry(TelemetryEvent {
                run: entry.id,
                tick: summary.tick,
                step_calls: summary.step_calls,
                faults: summary.faults,
                skipped: summary.skipped,
            });
            lock(&self.watchers).emit(entry.id, EventKinds::TELEMETRY, &event);
        }
        Ok(Reply::Stepped(summary))
    }

    fn take(&self, entry: &Arc<RunEntry>, port: &str) -> Answer {
        let mut run = lock(&entry.run);
        let host = run.host.host();
        // Checked before the value is taken, so a refusal leaves it in the port.
        let codec = self.port_codec(host, port, crate::Dir::Output, entry.id)?;
        let Some(value) = host.take_value(port).map_err(|error| port_error(&error))? else {
            return Ok(Reply::Value(None));
        };
        let wire = (codec.read)(&value).ok_or_else(|| {
            let key = port_key(host, port, crate::Dir::Output);
            refuse(
                ErrorCode::ValueType,
                std::format!("the value in `{port}` is not held as a `{key}`"),
            )
        })?;
        Ok(Reply::Value(Some(TypedValue::new(codec.ty.clone(), wire))))
    }

    fn reload(&self, session: &str, entry: &Arc<RunEntry>, name: &str) -> Answer {
        entry.live()?;
        let config = lock(&entry.run).config.clone();
        let bound = lock(&self.engine).bound(session, name, &config)?;
        let report = lock(&entry.run)
            .host
            .reload(bound)
            .map_err(|error| match error {
                ReloadError::OtherBridges { .. } => {
                    refuse(ErrorCode::InvalidRequest, error.to_string())
                }
                ReloadError::Load(_) => ServiceError::planning(error.to_string(), Vec::new()),
            })?;
        *lock(&entry.graph) = String::from(name);
        Ok(Reply::Reloaded(carry(&report)))
    }

    fn stop(&self, entry: &Arc<RunEntry>, mode: StopMode) -> Answer {
        if mode == StopMode::Drain {
            // Finish what is waiting: tick until a tick finds nothing to do. A graph that never
            // goes idle is stopped after the bound, and a cancelled run is not ticked at all.
            for _ in 0..self.options().drain_ticks {
                match entry.tick(self.now_ns()) {
                    Ok(ticked) if ticked.report.idle => break,
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
        entry.mark_stopped();
        entry.cancel();
        let removed = lock(&self.runs).remove(&entry.id).is_some();
        if !removed {
            return Err(refuse(
                ErrorCode::Stopped,
                std::format!("run {} was already stopped", entry.id),
            ));
        }
        let tick = lock(&entry.run).tick;
        let dropped = entry.queued();
        lock(&self.watchers).end_run(entry.id, EndReason::Stopped);
        Ok(Reply::Stopped {
            run: entry.id,
            tick,
            dropped,
        })
    }

    fn inspect(&self, session: &str, target: &InspectTarget) -> Result<Inspection, ServiceError> {
        Ok(match target {
            InspectTarget::Run(run) => Inspection::Run(self.run(*run)?.info()),
            InspectTarget::Telemetry(run) => Inspection::Telemetry(self.run(*run)?.telemetry()),
            InspectTarget::Graph(name) => self.inspect_graph(session, name)?,
            InspectTarget::Cache => self.inspect_cache()?,
            InspectTarget::Host => {
                let runs = lock(&self.runs).values().map(|run| run.brief()).collect();
                let engine = lock(&self.engine);
                Inspection::Host(HostInfo {
                    sessions: engine.stored_sessions(),
                    graphs: engine.stored_names(session),
                    runs,
                    plugins: engine
                        .loaded
                        .iter()
                        .cloned()
                        .chain(
                            engine
                                .plugins
                                .as_ref()
                                .map_or_else(Vec::new, |p| p.listing()),
                        )
                        .collect(),
                })
            }
        })
    }
}
