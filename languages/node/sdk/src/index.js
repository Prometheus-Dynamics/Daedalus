// The worker API: declare nodes with `node`, then `serve` them over the worker protocol. The
// service client is in `./client.js`, a separate entry, so a worker does not load it.
export { T, NodeError, WireError } from "./wire.js";
export { node, serve, NODES } from "./worker.js";
