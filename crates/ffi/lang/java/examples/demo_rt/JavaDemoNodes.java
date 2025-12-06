package daedalus.examples;

import daedalus.annotations.DefaultKind;
import daedalus.annotations.In;
import daedalus.annotations.Node;
import daedalus.annotations.Out;
import daedalus.annotations.ScalarType;

public final class JavaDemoNodes {
  private JavaDemoNodes() {}

  @Node(id = "demo_java_rt:add", label = "Add")
  @Out(index = 0, name = "out", scalar = ScalarType.Int)
  public static int add(
      @In(name = "lhs", scalar = ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = 2) int lhs,
      @In(name = "rhs", scalar = ScalarType.Int, defaultKind = DefaultKind.Int, defaultInt = 3) int rhs) {
    return lhs + rhs;
  }
}
