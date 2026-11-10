//! The stdio transport: one session, requests on one stream and answers on another. This is how
//! a parent process talks to a host it started.

use std::boxed::Box;
use std::io::{Read, Write};
use std::sync::Arc;

use super::ServiceHost;
use super::session::{Channel, serve};

impl ServiceHost {
    /// Serves one session on the process's stdin and stdout, until stdin closes. Nothing else
    /// may write to stdout while it runs.
    pub fn serve_stdio(self: &Arc<Self>) {
        self.serve_stream(std::io::stdin(), std::io::stdout());
    }

    /// Serves one session on any pair of streams, until the reader ends. Answers still queued
    /// when it ends are written before this returns.
    pub fn serve_stream(
        self: &Arc<Self>,
        reader: impl Read + Send + 'static,
        writer: impl Write + Send + 'static,
    ) {
        let channel = Channel {
            reader: Box::new(reader),
            writer: Box::new(writer),
            // There is nothing to shut down: the session ends when the reader does.
            close: Box::new(|| {}),
        };
        serve(self, channel, None);
    }
}
