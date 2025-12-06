package daedalus.example_project;

import daedalus.annotations.In;
import daedalus.annotations.InputPort;
import daedalus.annotations.Inputs;
import daedalus.annotations.Meta;
import daedalus.annotations.Node;
import daedalus.annotations.Out;
import daedalus.annotations.ScalarType;
import daedalus.annotations.State;
import daedalus.bridge.StateResult;
import daedalus.bridge.StatefulInvocation;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ExampleNodes {
  private ExampleNodes() {}

  @Node(
      id = "example_java:add",
      label = "Add",
      metadata = {@Meta(key = "lang", value = "java"), @Meta(key = "kind", value = "stateless")})
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int add(
      @In(name = "a", scalar = ScalarType.Int) int a,
      @In(name = "b", scalar = ScalarType.Int) int b) {
    return a + b;
  }

  @Node(
      id = "example_java:counter",
      label = "Counter",
      stateful = true,
      metadata = {@Meta(key = "lang", value = "java"), @Meta(key = "kind", value = "stateful")})
  @Inputs({@InputPort(name = "inc", scalar = ScalarType.Int)})
  @State(tyRef = "daedalus.example_project.ExampleTypes#counterStateTy")
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static StateResult counter(StatefulInvocation inv) {
    int inc = ((Number) inv.args.get(0)).intValue();

    int prev = 0;
    if (inv.state instanceof Map) {
      Object v = ((Map<?, ?>) inv.state).get("value");
      if (v instanceof Number) prev = ((Number) v).intValue();
    }

    int next = prev + inc;
    Map<String, Object> st = new LinkedHashMap<>();
    st.put("value", next);
    return new StateResult(st, next);
  }
}

