package dev.daedalus.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@DaedalusPlugin(
    id = "java_sdk_test",
    boundaryContracts = {@BoundaryContract(typeKey = "test.Point", capabilities = {"host_read", "worker_write"})})
final class PackageBuilderTestPlugin {
  static final class AccumState {
    long sum;
  }

  @TypeKey("test.Point")
  record Point(double x, double y) {}

  @Adapter(id = "test.point_to_i64", source = Point.class, target = Long.class)
  static long pointToI64(Point point) {
    return (long) point.x();
  }

  @Node(id = "add", inputs = {"a", "b"}, outputs = {"out"})
  static long add(long a, long b) {
    return a + b;
  }

  @Node(id = "accum", inputs = {"value"}, outputs = {"sum"}, state = AccumState.class)
  static long accum(long value, @State AccumState state) {
    return value;
  }

  @Node(id = "payload", inputs = {"frame"}, outputs = {"len"}, access = "view")
  static long payload(BytesView frame) {
    return frame.length();
  }

  @Node(
      id = "widths",
      inputs = {"b", "s", "c", "i", "l", "f", "d", "flag", "text", "count", "big", "id"},
      outputs = {"out"})
  @Scalar("u32")
  static long widths(
      byte b, Short s, char c, int i, Long l, float f, double d, boolean flag, String text,
      @Scalar("u32") long count, @Scalar("u64") long big, @Scalar("u8") int id) {
    return count;
  }

  @Node(id = "ratio", inputs = {"value"}, outputs = {"out"})
  static float ratio(@Scalar("f32") double value) {
    return (float) value;
  }
}

@DaedalusPlugin(id = "java_sdk_big_integer")
final class BigIntegerPlugin {
  @Node(id = "big", inputs = {"value"}, outputs = {"out"})
  static long big(java.math.BigInteger value) {
    return value.longValue();
  }
}

@DaedalusPlugin(id = "java_sdk_bad_scalar_carrier")
final class BadScalarCarrierPlugin {
  @Node(id = "bad", inputs = {"value"}, outputs = {"out"})
  static long bad(@Scalar("u32") double value) {
    return (long) value;
  }
}

@DaedalusPlugin(id = "java_sdk_unknown_scalar")
final class UnknownScalarPlugin {
  @Node(id = "bad", inputs = {"value"}, outputs = {"out"})
  static long bad(@Scalar("u128") long value) {
    return value;
  }
}

public final class PackageBuilderTest {
  public static void main(String[] args) throws Exception {
    PackageBuilder builder = PackageBuilder.fromAnnotatedPlugin(PackageBuilderTestPlugin.class)
        .classesDir("build/classes/java/main")
        .jar("build/libs/java-sdk-test.jar")
        .nativeLibrary("build/native/libjava_sdk_test.so");
    Map<String, Object> descriptor = builder.descriptorMap();
    PackageBuilder.validateDescriptor(descriptor);

    Map<?, ?> schema = (Map<?, ?>) descriptor.get("schema");
    List<?> nodes = (List<?>) schema.get("nodes");
    if (nodes.size() != 5) {
      throw new AssertionError("expected 5 nodes, found " + nodes.size());
    }
    expectPortTypes(node(nodes, "widths"), "inputs", Map.ofEntries(
        Map.entry("b", "I8"), Map.entry("s", "I16"), Map.entry("c", "U16"), Map.entry("i", "I32"),
        Map.entry("l", "Int"), Map.entry("f", "F32"), Map.entry("d", "Float"),
        Map.entry("flag", "Bool"), Map.entry("text", "String"), Map.entry("count", "U32"),
        Map.entry("big", "U64"), Map.entry("id", "U8")));
    expectPortTypes(node(nodes, "widths"), "outputs", Map.of("out", "U32"));
    expectPortTypes(node(nodes, "ratio"), "inputs", Map.of("value", "F32"));
    expectPortTypes(node(nodes, "ratio"), "outputs", Map.of("out", "F32"));
    expectPortTypes(node(nodes, "add"), "inputs", Map.of("a", "Int", "b", "Int"));
    expectPortTypes(node(nodes, "add"), "outputs", Map.of("out", "Int"));
    expectPortTypes(node(nodes, "payload"), "inputs", Map.of("frame", "Bytes"));
    expectBuildFailure(BigIntegerPlugin.class, "unsupported numeric type java.math.BigInteger");
    expectBuildFailure(BadScalarCarrierPlugin.class, "does not fit carrier type double");
    expectBuildFailure(UnknownScalarPlugin.class, "@Scalar(\"u128\")");
    Map<?, ?> backends = (Map<?, ?>) descriptor.get("backends");
    if (!backends.containsKey("add") || !backends.containsKey("accum") || !backends.containsKey("payload")) {
      throw new AssertionError("missing backend config");
    }
    if (!builder.descriptor().contains("\"package_builder\":\"dev.daedalus.plugin\"")) {
      throw new AssertionError("descriptor should be serializer generated with SDK metadata");
    }

    Path temp = Files.createTempFile("daedalus-java-sdk", ".json");
    builder.write(temp.toString());
    if (!Files.readString(temp).contains("\"java_sdk_test\"")) {
      throw new AssertionError("descriptor write failed");
    }
    Files.deleteIfExists(temp);

    Map<String, Object> bad = deepCopy(descriptor);
    ((Map<?, ?>) bad.get("backends")).remove("add");
    try {
      PackageBuilder.validateDescriptor(bad);
      throw new AssertionError("expected missing backend validation failure");
    } catch (IllegalArgumentException expected) {
      if (!expected.getMessage().contains("missing backend")) {
        throw expected;
      }
    }

    Map<String, Object> duplicateNode = deepCopy(descriptor);
    List<Object> duplicateNodes = (List<Object>) ((Map<String, Object>) duplicateNode.get("schema")).get("nodes");
    duplicateNodes.add(new java.util.LinkedHashMap<>((Map<String, Object>) duplicateNodes.get(0)));
    expectInvalid(duplicateNode, "duplicate");

    Map<String, Object> badAccess = deepCopy(descriptor);
    firstInput(badAccess).put("access", "project");
    expectInvalid(badAccess, "unsupported access");

    Map<String, Object> badResidency = deepCopy(descriptor);
    firstInput(badResidency).put("residency", "disk");
    expectInvalid(badResidency, "unsupported residency");

    Map<String, Object> badLayout = deepCopy(descriptor);
    Map<String, Object> badLayoutInput = firstInput(badLayout);
    badLayoutInput.remove("residency");
    badLayoutInput.put("layout", "rgba8-hwc");
    expectInvalid(badLayout, "layout requires residency");

    Map<String, Object> badContract = deepCopy(descriptor);
    Map<String, Object> badContractSchema = (Map<String, Object>) badContract.get("schema");
    Map<String, Object> contract =
        (Map<String, Object>) ((List<Object>) badContractSchema.get("boundary_contracts")).get(0);
    contract.put("type_key", "");
    expectInvalid(badContract, "type_key");
  }

  private static Map<?, ?> node(List<?> nodes, String id) {
    for (Object node : nodes) {
      if (id.equals(((Map<?, ?>) node).get("id"))) return (Map<?, ?>) node;
    }
    throw new AssertionError("missing node " + id);
  }

  private static void expectPortTypes(Map<?, ?> node, String direction, Map<String, String> expected) {
    for (Object value : (List<?>) node.get(direction)) {
      Map<?, ?> port = (Map<?, ?>) value;
      Object actual = ((Map<?, ?>) port.get("ty")).get("Scalar");
      if (!expected.get(String.valueOf(port.get("name"))).equals(actual)) {
        throw new AssertionError(node.get("id") + "." + port.get("name") + " expected "
            + expected.get(String.valueOf(port.get("name"))) + ", found " + port.get("ty"));
      }
    }
  }

  private static void expectBuildFailure(Class<?> plugin, String message) {
    try {
      PackageBuilder.fromAnnotatedPlugin(plugin).descriptorMap();
      throw new AssertionError("expected descriptor failure containing " + message);
    } catch (IllegalArgumentException expected) {
      if (!expected.getMessage().contains(message)) {
        throw expected;
      }
    }
  }

  private static Map<String, Object> deepCopy(Map<String, Object> value) {
    return (Map<String, Object>) copy(value);
  }

  private static Object copy(Object value) {
    if (value instanceof Map<?, ?> map) {
      Map<String, Object> copied = new java.util.LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        copied.put(String.valueOf(entry.getKey()), copy(entry.getValue()));
      }
      return copied;
    }
    if (value instanceof List<?> list) {
      List<Object> copied = new java.util.ArrayList<>();
      for (Object item : list) {
        copied.add(copy(item));
      }
      return copied;
    }
    return value;
  }

  private static Map<String, Object> firstInput(Map<String, Object> descriptor) {
    Map<String, Object> schema = (Map<String, Object>) descriptor.get("schema");
    List<Object> nodes = (List<Object>) schema.get("nodes");
    Map<String, Object> firstNode = (Map<String, Object>) nodes.get(0);
    return (Map<String, Object>) ((List<Object>) firstNode.get("inputs")).get(0);
  }

  private static void expectInvalid(Map<String, Object> descriptor, String message) {
    try {
      PackageBuilder.validateDescriptor(descriptor);
      throw new AssertionError("expected validation failure containing " + message);
    } catch (IllegalArgumentException expected) {
      if (!expected.getMessage().contains(message)) {
        throw expected;
      }
    }
  }
}
