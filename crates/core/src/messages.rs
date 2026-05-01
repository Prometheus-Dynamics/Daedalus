use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::clock::Tick;
use crate::ids::ChannelId;

/// Monotonic sequence number for newest-wins/broadcast helpers.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence(u64);

impl Sequence {
    pub const ZERO: Sequence = Sequence(0);

    pub fn new(value: u64) -> Self {
        Sequence(value)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Sequence(self.0.saturating_add(1))
    }
}

/// Unique token carried by every message.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Token(u64);

impl Token {
    pub fn new(raw: u64) -> Self {
        Token(raw)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

/// Generates monotonic tokens in a thread-safe manner.
///
#[derive(Debug, Default)]
pub struct TokenGenerator {
    counter: AtomicU64,
}

impl TokenGenerator {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    pub fn next(&self) -> Token {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        Token::new(id)
    }
}

/// Watermark used to signal progress on a stream.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    sequence: Sequence,
    tick: Tick,
}

impl Watermark {
    pub fn new(sequence: Sequence, tick: Tick) -> Self {
        Self { sequence, tick }
    }

    pub fn sequence(&self) -> Sequence {
        self.sequence
    }

    pub fn tick(&self) -> Tick {
        self.tick
    }
}

/// Metadata attached to messages for diagnostics/telemetry.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageMeta {
    pub created_at: Tick,
    pub source: Option<ChannelId>,
    pub sequence: Sequence,
}

impl MessageMeta {
    pub fn new(created_at: Tick, sequence: Sequence) -> Self {
        Self {
            created_at,
            source: None,
            sequence,
        }
    }

    pub fn with_source(mut self, source: ChannelId) -> Self {
        self.source = Some(source);
        self
    }
}

impl Default for MessageMeta {
    fn default() -> Self {
        Self {
            created_at: Tick::ZERO,
            source: None,
            sequence: Sequence::ZERO,
        }
    }
}

/// Envelope carrying payload plus token/metadata.
///
/// Compute `T` must be `Send + Sync` to be safely shared in async/concurrent
/// runtimes.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T: Send + Sync> {
    pub token: Token,
    pub meta: MessageMeta,
    pub payload: Arc<T>,
}

impl<T: Send + Sync> Message<T> {
    pub fn new(token: Token, meta: MessageMeta, payload: T) -> Self {
        Self {
            token,
            meta,
            payload: Arc::new(payload),
        }
    }

    /// Map payload while preserving token/metadata.
    pub fn map<U: Send + Sync>(self, f: impl FnOnce(Arc<T>) -> U) -> Message<U> {
        let payload = f(self.payload);
        Message {
            token: self.token,
            meta: self.meta,
            payload: Arc::new(payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TickClock;

    #[test]
    fn token_generator_monotonic() {
        let token_gen = TokenGenerator::new();
        let t1 = token_gen.next();
        let t2 = token_gen.next();
        assert!(t2.value() > t1.value());
    }

    #[test]
    fn message_round_trip() {
        let clock = TickClock::default();
        let token = Token::new(1);
        let meta = MessageMeta::new(clock.now_tick(), Sequence::ZERO);
        let msg = Message::new(token, meta, "payload");
        assert_eq!(msg.token.value(), 1);
        assert_eq!(msg.meta.sequence.value(), 0);
        assert_eq!(msg.payload.as_ref(), &"payload");
    }
}
