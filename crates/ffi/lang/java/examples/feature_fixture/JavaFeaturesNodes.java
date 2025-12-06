package daedalus.examples;

import daedalus.annotations.DefaultKind;
import daedalus.annotations.In;
import daedalus.annotations.Meta;
import daedalus.annotations.Node;
import daedalus.annotations.Out;
import daedalus.annotations.ScalarType;
import daedalus.annotations.State;
import daedalus.annotations.SyncGroup;
import daedalus.annotations.InputPort;
import daedalus.annotations.Inputs;
import daedalus.bridge.Extra;
import daedalus.bridge.StateResult;
import daedalus.bridge.StatefulInvocation;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JavaFeaturesNodes {
  private JavaFeaturesNodes() {}

  @Node(
      id = "demo_java_feat:add_defaults",
      label = "AddDefaults",
      metadata = {@Meta(key = "category", value = "math"), @Meta(key = "lang", value = "java")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int add_defaults(
      @In(name = "a", scalar = ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = 2) int a,
      @In(name = "b", scalar = ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = 3) int b) {
    return a + b;
  }

  @Node(
      id = "demo_java_feat:split",
      label = "Split",
      featureFlags = {"cpu"},
      metadata = {@Meta(key = "category", value = "math")})
  @Out(index = 0, name = "out0", scalar = ScalarType.Int)
  @Out(index = 1, name = "out1", scalar = ScalarType.Int)
  public static List<Object> split(@In(name = "value", scalar = ScalarType.Int) int value) {
    return Arrays.asList(value, -value);
  }

  @Node(id = "demo_java_feat:scale_cfg", label = "ScaleCfg", metadata = {@Meta(key = "category", value = "config")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int scale_cfg(
      @In(name = "value", scalar = ScalarType.Int) int value,
      @In(name = "cfg", tyRef = "daedalus.examples.JavaFeatureTypes#cfgTy") Map<String, Object> cfg) {
    Object factor = cfg != null ? cfg.get("factor") : null;
    int f = factor instanceof Number ? ((Number) factor).intValue() : 1;
    return value * f;
  }

  @Node(id = "demo_java_feat:make_point", label = "MakePoint", metadata = {@Meta(key = "category", value = "struct")})
  @Out(index = 0, name = "out", tyRef = "daedalus.examples.JavaFeatureTypes#pointTy")
  public static Map<String, Object> make_point(
      @In(name = "x", scalar = ScalarType.Int) int x, @In(name = "y", scalar = ScalarType.Int) int y) {
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("x", x);
    p.put("y", y);
    return p;
  }

  @Node(id = "demo_java_feat:enum_mode", label = "EnumMode", metadata = {@Meta(key = "category", value = "enum")})
  @Out(index = 0, name = "out", tyRef = "daedalus.examples.JavaFeatureTypes#modeTy")
  public static Map<String, Object> enum_mode(@In(name = "value", scalar = ScalarType.Int) int value) {
    Map<String, Object> ev = new LinkedHashMap<>();
    if ((value & 1) == 0) {
      ev.put("name", "A");
      ev.put("value", value);
      return ev;
    }
    ev.put("name", "B");
    ev.put("value", make_point(value, value + 1));
    return ev;
  }

  @Node(
      id = "demo_java_feat:sync_a_only",
      label = "SyncAOnly",
      syncGroups = {@SyncGroup(ports = {"a"})},
      metadata = {@Meta(key = "category", value = "sync")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int sync_a_only(
      @In(name = "a", scalar = ScalarType.Int) int a,
      @In(name = "b", tyRef = "daedalus.examples.JavaFeatureTypes#optionalIntTy") Integer b) {
    return a;
  }

  @Node(
      id = "demo_java_feat:sync_a_only_obj",
      label = "SyncAOnlyObj",
      syncGroups = {
        @SyncGroup(
            name = "a_only",
            ports = {"a"},
            policy = daedalus.manifest.SyncPolicy.Latest,
            backpressure = daedalus.manifest.BackpressureStrategy.ErrorOnOverflow,
            capacity = 2)
      },
      metadata = {@Meta(key = "category", value = "sync")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int sync_a_only_obj(
      @In(name = "a", scalar = ScalarType.Int) int a,
      @In(name = "b", tyRef = "daedalus.examples.JavaFeatureTypes#optionalIntTy") Integer b) {
    return a;
  }

  @Node(id = "demo_java_feat:ctx_echo", label = "CtxEcho", metadata = {@Meta(key = "category", value = "ctx")})
  @Out(index = 0, name = "out", scalar = ScalarType.String)
  public static String ctx_echo(@In(name = "text", scalar = ScalarType.String) String text, Extra extra) {
    String nodeId = String.valueOf(extra.node.get("id"));
    return text + "|" + nodeId;
  }

  @Node(id = "demo_java_feat:choose_mode_meta", label = "ChooseModeMeta", metadata = {@Meta(key = "category", value = "meta")})
  @Out(index = 0, name = "out", scalar = ScalarType.String)
  public static String choose_mode_meta(
      @In(
              name = "mode",
              scalar = ScalarType.String,
              defaultKind = DefaultKind.String,
              defaultString = "quality",
              source = "modes")
          String mode) {
    return mode;
  }

  @Node(id = "demo_java_feat:accum", label = "Accum", stateful = true, metadata = {@Meta(key = "category", value = "state")})
  @Inputs({@InputPort(name = "value", scalar = ScalarType.Int)})
  @State(tyRef = "daedalus.examples.JavaFeatureTypes#accumStateTy")
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static StateResult accum(StatefulInvocation inv) {
    int value = ((Number) inv.args.get(0)).intValue();

    int total = 0;
    if (inv.state instanceof Map) {
      Object v = ((Map<?, ?>) inv.state).get("total");
      if (v instanceof Number) total = ((Number) v).intValue();
    }

    total += value;
    Map<String, Object> st = new LinkedHashMap<>();
    st.put("total", total);
    return new StateResult(st, total);
  }

  @Node(id = "demo_java_feat:multi_emit", label = "MultiEmit", rawIo = true, metadata = {@Meta(key = "category", value = "raw_io")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static Object multi_emit(Extra extra) {
    if (extra.io == null) throw new IllegalStateException("raw_io requires Extra.io");
    extra.io.push("out", 1);
    extra.io.push("out", 2);
    return null;
  }
}
