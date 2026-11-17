// Packet framing for a stream: a u32 length, then the body. A framer takes bytes as they come and
// gives whole packets, in order. It keeps what it has not given out, so a reader can take over a
// stream from another reader with the bytes that came after the last packet it gave out.

import * as wire from "./wire.js";

export class Framer {
  /**
   * @param {number} cap the largest body accepted; a longer one is refused, as the host refuses it.
   * @param {Buffer} [rest] bytes already received, before any bytes pushed.
   */
  constructor(cap, rest = Buffer.alloc(0)) {
    this.cap = cap;
    this.buf = rest;
    this.at = 0;
  }

  /** Adds received bytes. */
  push(chunk) {
    this.buf = this.at === 0 && this.buf.length === 0 ? chunk : Buffer.concat([this.buf.subarray(this.at), chunk]);
    this.at = 0;
  }

  /** The next whole packet's body, or null when the next one is not all here. Throws when it is too long. */
  next() {
    if (this.buf.length - this.at < 4) return null;
    const len = this.buf.readUInt32LE(this.at);
    if (len > this.cap) throw new wire.WireError(`packet of ${len} bytes is over the cap of ${this.cap}`);
    if (this.buf.length - this.at - 4 < len) return null;
    const body = this.buf.subarray(this.at + 4, this.at + 4 + len);
    this.at += 4 + len;
    return body;
  }

  /** Every byte received that is not yet part of a packet given out. */
  rest() {
    return this.buf.subarray(this.at);
  }
}
