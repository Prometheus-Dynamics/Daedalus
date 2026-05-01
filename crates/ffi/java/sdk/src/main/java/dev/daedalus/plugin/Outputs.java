package dev.daedalus.plugin;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Outputs {
  private final Map<String, Object> values = new LinkedHashMap<>();

  private Outputs(Object... pairs) {
    for (int i = 0; i + 1 < pairs.length; i += 2) {
      values.put(String.valueOf(pairs[i]), pairs[i + 1]);
    }
  }

  public static Outputs of(Object... pairs) {
    return new Outputs(pairs);
  }

  public Map<String, Object> values() {
    return values;
  }
}
