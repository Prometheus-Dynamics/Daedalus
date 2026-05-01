package dev.daedalus.plugin;

@FunctionalInterface
public interface PixelMapper {
  int[] map(int r, int g, int b, int a);
}
