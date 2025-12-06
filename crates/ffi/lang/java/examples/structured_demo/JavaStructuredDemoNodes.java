package daedalus.examples;

import daedalus.annotations.DefaultKind;
import daedalus.annotations.In;
import daedalus.annotations.Node;
import daedalus.annotations.Out;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class JavaStructuredDemoNodes {
  private JavaStructuredDemoNodes() {}

  @Node(id = "demo_java_struct:translate_point", label = "TranslatePoint")
  @Out(index = 0, name = "out", tyRef = "daedalus.examples.JavaStructuredDemoTypes#pointTy")
  public static Map<String, Object> translate_point(
      @In(name = "pt", tyRef = "daedalus.examples.JavaStructuredDemoTypes#pointTy") Map<String, Object> pt,
      @In(name = "dx", scalar = daedalus.annotations.ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = 1)
          int dx,
      @In(name = "dy", scalar = daedalus.annotations.ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = -1)
          int dy) {
    Map<String, Object> out = new HashMap<>();
    out.put("x", ((Number) pt.get("x")).intValue() + dx);
    out.put("y", ((Number) pt.get("y")).intValue() + dy);
    return out;
  }

  @Node(id = "demo_java_struct:flip_mode", label = "FlipMode")
  @Out(index = 0, name = "out", tyRef = "daedalus.examples.JavaStructuredDemoTypes#modeTy")
  public static Map<String, Object> flip_mode(
      @In(name = "mode", tyRef = "daedalus.examples.JavaStructuredDemoTypes#modeTy") Map<String, Object> mode) {
    Object name = mode.get("name");
    if ("A".equals(name)) {
      Map<String, Object> out = new HashMap<>();
      out.put("name", "B");
      out.put("value", Map.of("x", 7, "y", 9));
      return out;
    }
    return Map.of("name", "A", "value", 1);
  }

  @Node(id = "demo_java_struct:map_len", label = "MapLen")
  @Out(index = 0, name = "out", scalar = daedalus.annotations.ScalarType.Int)
  public static int map_len(
      @In(name = "m", tyRef = "daedalus.examples.JavaStructuredDemoTypes#mapStringIntTy") Map<String, Object> m) {
    return m.size();
  }

  @Node(id = "demo_java_struct:list_sum", label = "ListSum")
  @Out(index = 0, name = "out", scalar = daedalus.annotations.ScalarType.Int)
  public static int list_sum(
      @In(name = "items", tyRef = "daedalus.examples.JavaStructuredDemoTypes#listIntTy") List<Object> items) {
    int sum = 0;
    for (Object v : items) {
      sum += ((Number) v).intValue();
    }
    return sum;
  }
}
