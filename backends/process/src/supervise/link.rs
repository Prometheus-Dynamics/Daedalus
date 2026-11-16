//! The worker's link: up, being checked, or down until its backoff has passed. Bringing it up
//! is here. A check of a worker's files runs on a thread of its own, so the tick only asks
//! whether it is done and never hashes a file.

use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use daedalus_integrity::{IntegrityError, Mode};
use daedalus_wire::{Hello, Schema};

use super::Shared;
use crate::backoff::delay_after;
use crate::child::{Got, Worker};
use crate::telemetry::Count;
use crate::vet::{Vetted, vet};

/// What the link is doing right now.
pub(crate) enum Link {
    Up(Worker),
    /// The worker's files are being checked on a thread of their own. Nothing runs until the
    /// check has said.
    Checking(Receiver<Result<Vetted, IntegrityError>>),
    /// Down until the backoff has passed. Nothing runs before then.
    Down {
        until: Instant,
    },
}

/// Why a link is not up.
pub(crate) enum NotUp {
    /// The worker failed a moment ago and its next start isn't due. Not a new failure.
    BackingOff(String),
    /// The worker's files are being checked. Not a new failure.
    Checking(String),
    /// The worker was started, or its files refused, and it did not come up.
    Start(String),
}

/// Brings the link up. A down link waits for its backoff, then starts the worker again. Under
/// Locked or Record the worker's files are checked first, on a thread of their own.
pub(crate) fn ensure_up(shared: &Shared, sup: &mut super::Supervisor) -> Result<(), NotUp> {
    let until = match &sup.link {
        Link::Up(_) => return Ok(()),
        Link::Checking(_) => return poll_check(shared, sup),
        Link::Down { until } => *until,
    };
    let now = Instant::now();
    if now < until {
        return Err(NotUp::BackingOff(format!(
            "worker {} is backing off for {:?}",
            shared.spec.program.display(),
            until - now
        )));
    }
    if shared.integrity.mode() != Mode::Off {
        return begin_check(shared, sup);
    }
    match start(shared, &Vetted::default()) {
        Ok(worker) => {
            shared.telemetry.bump(Count::Restart);
            sup.link = Link::Up(worker);
            Ok(())
        }
        Err(reason) => {
            sup.failures = sup.failures.saturating_add(1);
            sup.link = Link::Down {
                until: Instant::now() + delay_after(sup.failures),
            };
            Err(NotUp::Start(reason))
        }
    }
}

/// Starts a thread that checks the worker's files, and leaves the link checking.
fn begin_check(shared: &Shared, sup: &mut super::Supervisor) -> Result<(), NotUp> {
    let (sender, receiver) = mpsc::channel();
    let integrity = shared.integrity.clone();
    let spec = shared.spec.clone();
    let spawned = thread::Builder::new()
        .name("daedalus-integrity".into())
        .spawn(move || {
            // The receiver is gone only if the link was dropped, and then nobody needs the answer.
            let _ = sender.send(vet(&integrity, &spec));
        });
    match spawned {
        Ok(_) => {
            sup.link = Link::Checking(receiver);
            Err(NotUp::Checking(format!(
                "checking the files of worker {} against the lock",
                shared.spec.program.display()
            )))
        }
        Err(error) => Err(refused(
            shared,
            sup,
            format!("could not start the check of the worker's files: {error}"),
        )),
    }
}

/// Asks the check whether it has finished, and starts the worker if it has.
fn poll_check(shared: &Shared, sup: &mut super::Supervisor) -> Result<(), NotUp> {
    let polled = match &sup.link {
        Link::Checking(receiver) => receiver.try_recv(),
        _ => return Ok(()),
    };
    match polled {
        Err(TryRecvError::Empty) => Err(NotUp::Checking(format!(
            "checking the files of worker {} against the lock",
            shared.spec.program.display()
        ))),
        Ok(Ok(vetted)) => match start(shared, &vetted) {
            Ok(worker) => {
                shared.telemetry.bump(Count::Restart);
                sup.link = Link::Up(worker);
                Ok(())
            }
            Err(reason) => {
                sup.failures = sup.failures.saturating_add(1);
                sup.link = Link::Down {
                    until: Instant::now() + delay_after(sup.failures),
                };
                Err(NotUp::Start(reason))
            }
        },
        Ok(Err(error)) => Err(refused(shared, sup, error.to_string())),
        Err(TryRecvError::Disconnected) => Err(refused(
            shared,
            sup,
            format!(
                "the check of worker {} ended without an answer",
                shared.spec.program.display()
            ),
        )),
    }
}

/// A check refused the worker's files: the link goes down until the backoff has passed, and the
/// next start checks the files again, from the lock as it is then.
fn refused(shared: &Shared, sup: &mut super::Supervisor, reason: String) -> NotUp {
    shared.telemetry.bump(Count::Refused);
    sup.failures = sup.failures.saturating_add(1);
    sup.link = Link::Down {
        until: Instant::now() + delay_after(sup.failures),
    };
    NotUp::Start(reason)
}

/// Starts a worker from `vetted` and reads its hello. A hello with another layout is refused.
fn start(shared: &Shared, vetted: &Vetted) -> Result<Worker, String> {
    let mut worker = Worker::spawn(&shared.spec, vetted.program.as_ref(), &shared.telemetry)
        .map_err(|error| error.to_string())?;
    let hello = read_hello(&worker, shared.spec.startup)?;
    if let Err(error) = hello.check() {
        worker.kill();
        return Err(format!("{}: {error}", shared.spec.program.display()));
    }
    // A worker started again after one died or was killed is lent memory anew.
    if let Err(reason) = worker.offer(shared.spec.startup, &shared.telemetry) {
        worker.kill();
        return Err(format!("{}: {reason}", shared.spec.program.display()));
    }
    #[cfg(target_os = "linux")]
    worker.point_rooms(&shared.rooms);
    if let Err(error) = vetted.record() {
        worker.kill();
        return Err(error.to_string());
    }
    Ok(worker)
}

pub(crate) fn read_hello(worker: &Worker, startup: Duration) -> Result<Hello, String> {
    match worker.receive(Some(startup)) {
        Got::Packet(buf) => {
            let decoded = daedalus_wire::WorkerMsg::decode(&buf, &Schema::new());
            worker.give_back(buf);
            match decoded {
                Ok(daedalus_wire::WorkerMsg::Hello(hello)) => Ok(hello),
                Ok(_) => Err("the worker's first message was not a hello".into()),
                Err(error) => Err(format!("the worker's hello did not decode: {error}")),
            }
        }
        Got::InBuffer(_) => Err("the worker's first message was not a hello".into()),
        Got::Broken(error) => Err(format!("the worker closed before its hello: {error}")),
        Got::Timeout => Err(format!("no hello within {startup:?}")),
    }
}
