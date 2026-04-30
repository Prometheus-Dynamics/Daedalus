package daedalus.manifest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ManifestBuilders {
  private ManifestBuilders() {}

  public static Map<String, Object> port(String name, Map<String, Object> ty) {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("name", name);
    p.put("ty", ty);
    return p;
  }

  public static Map<String, Object> port(
      String name, Map<String, Object> ty, Object constValue, String source) {
    Map<String, Object> p = port(name, ty);
    if (source != null) p.put("source", source);
    if (constValue != null) p.put("const_value", constValue);
    return p;
  }

  public static Map<String, Object> syncGroup(List<String> ports) {
    List<String> copy = new ArrayList<>(ports);
    Map<String, Object> g = new LinkedHashMap<>();
    g.put("ports", copy);
    g.put("policy", SyncPolicy.AllReady);
    return g;
  }

  public static Map<String, Object> syncGroup(
      String name,
      List<String> ports,
      SyncPolicy policy,
      BackpressureStrategy backpressure,
      Integer capacity) {
    Map<String, Object> g = new LinkedHashMap<>();
    if (name != null) g.put("name", name);
    g.put("ports", new ArrayList<>(ports));
    g.put("policy", policy != null ? policy : SyncPolicy.AllReady);
    if (backpressure != null) g.put("backpressure", backpressure);
    if (capacity != null) g.put("capacity", capacity);
    return g;
  }

}
