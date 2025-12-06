package daedalus.examples;

import daedalus.manifest.Types;

import java.util.Arrays;
import java.util.Map;

public final class JavaFeatureTypes {
  private JavaFeatureTypes() {}

  public static Map<String, Object> cfgTy() {
    return Types.structTy(Arrays.asList(new Types.Field("factor", Types.intTy())));
  }

  public static Map<String, Object> pointTy() {
    return Types.structTy(Arrays.asList(new Types.Field("x", Types.intTy()), new Types.Field("y", Types.intTy())));
  }

  public static Map<String, Object> modeTy() {
    return Types.enumTy(Arrays.asList(new Types.Variant("A", Types.intTy()), new Types.Variant("B", pointTy())));
  }

  public static Map<String, Object> accumStateTy() {
    return Types.structTy(Arrays.asList(new Types.Field("total", Types.intTy())));
  }

  public static Map<String, Object> optionalIntTy() {
    return Types.optional(Types.intTy());
  }

  public static Map<String, Object> imageTy() {
    return Types.structTy(
        Arrays.asList(
            new Types.Field("data_b64", Types.stringTy()),
            new Types.Field("width", Types.intTy()),
            new Types.Field("height", Types.intTy()),
            new Types.Field("channels", Types.intTy()),
            new Types.Field("dtype", Types.stringTy()),
            new Types.Field("layout", Types.stringTy())));
  }
}
