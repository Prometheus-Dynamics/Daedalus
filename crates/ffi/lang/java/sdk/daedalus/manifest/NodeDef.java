package daedalus.manifest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class NodeDef {
  public final String id;
  public String label = null;

  public String java_classpath = null;
  public String java_class = null;
  public String java_method = null;

  public boolean raw_io = false;
  public boolean stateful = false;
  public Object state = null;
  public String capability = null;
  public Object shader = null;

  public List<String> feature_flags = new ArrayList<>();
  public String default_compute = "CpuOnly";
  public List<Object> sync_groups = new ArrayList<>();
  public Map<String, Object> metadata = new LinkedHashMap<>();
  public List<Object> inputs = new ArrayList<>();
  public List<Object> outputs = new ArrayList<>();

  public NodeDef(String id) {
    this.id = id;
  }

  public NodeDef javaEntrypoint(String classpath, String cls, String method) {
    this.java_classpath = classpath;
    this.java_class = cls;
    this.java_method = method;
    return this;
  }

  public NodeDef input(Map<String, Object> port) {
    this.inputs.add(port);
    return this;
  }

  public NodeDef output(Map<String, Object> port) {
    this.outputs.add(port);
    return this;
  }

  public Map<String, Object> toManifest() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", id);
    if (label != null) m.put("label", label);
    if (java_classpath != null) m.put("java_classpath", java_classpath);
    if (java_class != null) m.put("java_class", java_class);
    if (java_method != null) m.put("java_method", java_method);
    if (raw_io) m.put("raw_io", true);
    if (stateful) m.put("stateful", true);
    if (state != null) m.put("state", state);
    if (capability != null) m.put("capability", capability);
    if (shader != null) m.put("shader", shader);
    if (feature_flags != null && !feature_flags.isEmpty()) m.put("feature_flags", feature_flags);
    if (default_compute != null) m.put("default_compute", default_compute);
    if (sync_groups != null && !sync_groups.isEmpty()) m.put("sync_groups", sync_groups);
    if (metadata != null && !metadata.isEmpty()) m.put("metadata", metadata);
    if (inputs != null && !inputs.isEmpty()) m.put("inputs", inputs);
    if (outputs != null && !outputs.isEmpty()) m.put("outputs", outputs);
    return m;
  }
}

