package dev.daedalus.plugin;

import java.util.Arrays;

public final class CowBytes extends BytesView {
  public CowBytes(byte[] bytes) {
    super(bytes);
  }

  public CowBytes withAppended(byte value) {
    byte[] copy = Arrays.copyOf(bytes, bytes.length + 1);
    copy[copy.length - 1] = value;
    return new CowBytes(copy);
  }
}
