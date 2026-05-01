package dev.daedalus.plugin;

public final class MutableRgba8Image extends Rgba8Image {
  public MutableRgba8Image mapPixelsInPlace(PixelMapper mapper) {
    return this;
  }
}
