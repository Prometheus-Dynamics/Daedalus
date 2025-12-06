package daedalus.examples;

import daedalus.manifest.Types;

import java.util.List;
import java.util.Map;

/** TypeExpr helpers used by {@link JavaStructuredDemoNodes}. */
public final class JavaStructuredDemoTypes {
  private JavaStructuredDemoTypes() {}

  public static Map<String, Object> pointTy() {
    return Types.structTy(
        List.of(
            new Types.Field("x", Types.intTy()),
            new Types.Field("y", Types.intTy())));
  }

  public static Map<String, Object> modeTy() {
    return Types.enumTy(
        List.of(
            new Types.Variant("A", Types.intTy()),
            new Types.Variant("B", pointTy())));
  }

  public static Map<String, Object> mapStringIntTy() {
    return Types.map(Types.stringTy(), Types.intTy());
  }

  public static Map<String, Object> listIntTy() {
    return Types.list(Types.intTy());
  }
}

