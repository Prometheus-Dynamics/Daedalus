package daedalus.example_project;

import daedalus.manifest.Types;

import java.util.Arrays;
import java.util.Map;

public final class ExampleTypes {
  private ExampleTypes() {}

  // Referenced by tyRef ("ClassName#method") so the annotation emitter can load it reflectively.
  public static Map<String, Object> intType() {
    return Types.intTy();
  }

  public static Map<String, Object> counterStateTy() {
    return Types.structTy(Arrays.asList(new Types.Field("value", Types.intTy())));
  }
}

