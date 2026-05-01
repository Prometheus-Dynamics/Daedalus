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
    if (nodes.size() != 3) {
      throw new AssertionError("expected 3 nodes, found " + nodes.size());
    }
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
