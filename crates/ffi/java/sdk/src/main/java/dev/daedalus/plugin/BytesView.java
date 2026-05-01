package dev.daedalus.plugin;

public class BytesView {
  protected final byte[] bytes;

  public BytesView(byte[] bytes) {
    this.bytes = bytes;
  }

  public long length() {
    return bytes.length;
  }
}
