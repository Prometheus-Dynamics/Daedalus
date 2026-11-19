//! What a driver's commands call. The reference calls `core/client`; a driver over another surface
//! of the same client (the client C ABI) gives its own `Service`, and the commands, their parsing
//! and their answer lines stay the reference's.

use daedalus_client::protocol::{
    CarryReport, Diagnostic, ErrorCode, Event, EventKinds, InspectTarget, Inspection, PlanOptions,
    PlanSummary, PluginInfo, StartConfig, StepSummary, StopMode, TraceEntry, TypedValue,
    WatchTarget,
};
use daedalus_client::{Client, ClientError, Fed, Stopped, Watch};

/// Why a call gave no result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The host refused it, with this code.
    Code(ErrorCode),
    /// Anything else: the connection, the codec, a buffer.
    Local,
}

/// A connection to the host, in one session. Each method is one request. A clone is the same
/// connection in the same session.
pub trait Service: Sized + Clone {
    type Watch: Events;

    fn in_session(&self, name: &str) -> Result<Self, Refused>;
    fn ping(&self) -> Result<u32, Refused>;
    fn load_plugin(&self, path: &str) -> Result<PluginInfo, Refused>;
    fn put_graph(&self, name: &str, json: &str) -> Result<Vec<Diagnostic>, Refused>;
    fn patch_graph(&self, name: &str, patch: &str) -> Result<Vec<Diagnostic>, Refused>;
    fn plan(&self, graph: &str, options: PlanOptions) -> Result<PlanSummary, Refused>;
    fn make_plan(&self, graph: &str, config: StartConfig) -> Result<(u64, Vec<u8>), Refused>;
    fn put_plan(&self, name: &str, bytes: &[u8]) -> Result<(), Refused>;
    fn start(&self, graph: &str, config: StartConfig) -> Result<u64, Refused>;
    fn feed(&self, run: u64, port: &str, value: TypedValue) -> Result<Fed, Refused>;
    fn step(&self, run: u64, count: u32) -> Result<StepSummary, Refused>;
    fn take(&self, run: u64, port: &str) -> Result<Option<TypedValue>, Refused>;
    fn watch(&self, target: WatchTarget, kinds: EventKinds) -> Result<Self::Watch, Refused>;
    fn reload(&self, run: u64, graph: &str) -> Result<CarryReport, Refused>;
    fn cancel(&self, run: u64) -> Result<(), Refused>;
    fn stop(&self, run: u64, mode: StopMode) -> Result<Stopped, Refused>;
    fn inspect(&self, target: InspectTarget) -> Result<Inspection, Refused>;
    fn cache_clear(&self) -> Result<(), Refused>;
    fn trace(&self, limit: u32) -> Result<Vec<TraceEntry>, Refused>;
}

/// A watch: its events in the order the host sent them.
pub trait Events {
    /// The next event, waiting for it; `None` once the watch has no more.
    fn next_event(&self) -> Option<Event>;
    /// Ends the subscription. Events already queued can still be read.
    fn close(&self);
}

fn refused(error: ClientError) -> Refused {
    match error {
        ClientError::Service(error) => Refused::Code(error.code),
        _ => Refused::Local,
    }
}

impl Service for Client {
    type Watch = Watch;

    fn in_session(&self, name: &str) -> Result<Client, Refused> {
        Client::in_session(self, name).map_err(refused)
    }
    fn ping(&self) -> Result<u32, Refused> {
        Client::ping(self).map_err(refused)
    }
    fn load_plugin(&self, path: &str) -> Result<PluginInfo, Refused> {
        Client::load_plugin(self, path).map_err(refused)
    }
    fn put_graph(&self, name: &str, json: &str) -> Result<Vec<Diagnostic>, Refused> {
        Client::put_graph(self, name, json).map_err(refused)
    }
    fn patch_graph(&self, name: &str, patch: &str) -> Result<Vec<Diagnostic>, Refused> {
        Client::patch_graph(self, name, patch).map_err(refused)
    }
    fn plan(&self, graph: &str, options: PlanOptions) -> Result<PlanSummary, Refused> {
        Client::plan(self, graph, options).map_err(refused)
    }
    fn make_plan(&self, graph: &str, config: StartConfig) -> Result<(u64, Vec<u8>), Refused> {
        Client::make_plan(self, graph, config).map_err(refused)
    }
    fn put_plan(&self, name: &str, bytes: &[u8]) -> Result<(), Refused> {
        Client::put_plan(self, name, bytes).map_err(refused)
    }
    fn start(&self, graph: &str, config: StartConfig) -> Result<u64, Refused> {
        Client::start(self, graph, config).map_err(refused)
    }
    fn feed(&self, run: u64, port: &str, value: TypedValue) -> Result<Fed, Refused> {
        Client::feed(self, run, port, value).map_err(refused)
    }
    fn step(&self, run: u64, count: u32) -> Result<StepSummary, Refused> {
        Client::step(self, run, count).map_err(refused)
    }
    fn take(&self, run: u64, port: &str) -> Result<Option<TypedValue>, Refused> {
        Client::take(self, run, port).map_err(refused)
    }
    fn watch(&self, target: WatchTarget, kinds: EventKinds) -> Result<Watch, Refused> {
        Client::watch(self, target, kinds).map_err(refused)
    }
    fn reload(&self, run: u64, graph: &str) -> Result<CarryReport, Refused> {
        Client::reload(self, run, graph).map_err(refused)
    }
    fn cancel(&self, run: u64) -> Result<(), Refused> {
        Client::cancel(self, run).map_err(refused)
    }
    fn stop(&self, run: u64, mode: StopMode) -> Result<Stopped, Refused> {
        Client::stop(self, run, mode).map_err(refused)
    }
    fn inspect(&self, target: InspectTarget) -> Result<Inspection, Refused> {
        Client::inspect(self, target).map_err(refused)
    }
    fn cache_clear(&self) -> Result<(), Refused> {
        Client::cache_clear(self).map_err(refused)
    }
    fn trace(&self, limit: u32) -> Result<Vec<TraceEntry>, Refused> {
        Client::trace(self, limit).map_err(refused)
    }
}

impl Events for Watch {
    fn next_event(&self) -> Option<Event> {
        Watch::next_event(self)
    }
    fn close(&self) {
        Watch::close(self)
    }
}
