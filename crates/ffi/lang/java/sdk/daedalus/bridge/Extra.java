package daedalus.bridge;

import java.util.Map;

public final class Extra {
  public final Map<String, Object> ctx;
  public final Map<String, Object> node;
  public final RawIo io;

  public Extra(Map<String, Object> ctx, Map<String, Object> node, RawIo io) {
    this.ctx = ctx;
    this.node = node;
    this.io = io;
  }
}
