package dev.daedalus.plugin;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PackageBuilder {
  private static final Set<String> VALID_ACCESS = Set.of("read", "view", "modify", "move");
  private static final Set<String> VALID_RESIDENCY = Set.of("cpu", "gpu");
  private static final Set<String> VALID_BOUNDARY_CAPABILITIES =
      Set.of("host_read", "worker_write", "borrow_ref", "borrow_mut", "shared_clone");
  private final Class<?> pluginClass;
  private final List<String> classesDirs = new ArrayList<>();
  private final List<String> jars = new ArrayList<>();
  private final List<String> nativeLibraries = new ArrayList<>();

  private PackageBuilder(Class<?> pluginClass) {
    this.pluginClass = pluginClass;
  }

  public static PackageBuilder fromAnnotatedPlugin(Class<?> pluginClass) {
    return new PackageBuilder(pluginClass);
  }

  public PackageBuilder classesDir(String path) {
    classesDirs.add(path);
    return this;
  }

  public PackageBuilder jar(String path) {
    jars.add(path);
    return this;
  }

  public PackageBuilder nativeLibrary(String path) {
    nativeLibraries.add(path);
    return this;
  }

  public void write(String path) throws IOException {
    Files.writeString(Path.of(path), descriptor());
  }

  public String descriptor() {
    Map<String, Object> descriptor = descriptorMap();
    return Json.write(descriptor) + "\n";
  }

  public Map<String, Object> descriptorMap() {
    DaedalusPlugin plugin = pluginClass.getAnnotation(DaedalusPlugin.class);
    if (plugin == null) {
      throw new IllegalArgumentException("plugin class is missing @DaedalusPlugin");
    }

    List<Method> nodeMethods = nodeMethods();
    Map<String, Object> descriptor = new LinkedHashMap<>();
    Map<String, Object> schema = new LinkedHashMap<>();
    Map<String, Object> pluginInfo = new LinkedHashMap<>();
    Map<String, Object> backends = new LinkedHashMap<>();
    List<Object> nodes = new ArrayList<>();

    pluginInfo.put("name", plugin.id());
    pluginInfo.put("version", "1.0.0");
    pluginInfo.put("description", null);
    pluginInfo.put("metadata", Map.of());

    for (Method method : nodeMethods) {
      Node node = method.getAnnotation(Node.class);
      nodes.add(nodeMap(method));
      backends.put(node.id(), backendMap(method, node));
    }

    schema.put("schema_version", 1);
    schema.put("plugin", pluginInfo);
    schema.put("dependencies", List.of());
    schema.put("required_host_capabilities", List.of());
    schema.put("feature_flags", List.of());
    schema.put("boundary_contracts", boundaryContracts(plugin.boundaryContracts()));
    schema.put("nodes", nodes);

    descriptor.put("schema_version", 1);
    descriptor.put("schema", schema);
    descriptor.put("backends", backends);
    descriptor.put("artifacts", artifacts());
    descriptor.put("lockfile", "plugin.lock.json");
    descriptor.put("manifest_hash", null);
    descriptor.put("signature", null);
    descriptor.put("metadata", metadata());
    validateDescriptor(descriptor);
    return descriptor;
  }

  public static void validateDescriptor(Map<String, Object> descriptor) {
    if (!Integer.valueOf(1).equals(descriptor.get("schema_version"))) {
      throw new IllegalArgumentException("unsupported schema_version");
    }
    Object schemaValue = descriptor.get("schema");
    Object backendsValue = descriptor.get("backends");
    if (!(schemaValue instanceof Map<?, ?> schema) || !(backendsValue instanceof Map<?, ?> backends)) {
      throw new IllegalArgumentException("descriptor is missing schema or backends");
    }
    Object nodesValue = schema.get("nodes");
    if (!(nodesValue instanceof List<?> nodes)) {
      throw new IllegalArgumentException("descriptor is missing nodes");
    }
    List<String> ids = new ArrayList<>();
    for (Object value : nodes) {
      if (!(value instanceof Map<?, ?> node)) {
        throw new IllegalArgumentException("node entry must be an object");
      }
      String id = String.valueOf(node.get("id"));
      if (id.isBlank() || ids.contains(id)) {
        throw new IllegalArgumentException("duplicate or missing node id: " + id);
      }
      ids.add(id);
      if (!"java".equals(node.get("backend"))) {
        throw new IllegalArgumentException("node `" + id + "` must use java backend");
      }
      validatePorts(id, node.get("inputs"));
      validatePorts(id, node.get("outputs"));
      Object backendValue = backends.get(id);
      if (!(backendValue instanceof Map<?, ?> backend)) {
        throw new IllegalArgumentException("node `" + id + "` is missing backend config");
      }
      if (!"java".equals(backend.get("backend"))) {
        throw new IllegalArgumentException("node `" + id + "` backend config must use java");
      }
      if (!"persistent_worker".equals(backend.get("runtime_model"))) {
        throw new IllegalArgumentException("node `" + id + "` must use persistent_worker");
      }
    }
    validateBoundaryContracts(schema.get("boundary_contracts"));
    for (Object key : backends.keySet()) {
      if (!ids.contains(String.valueOf(key))) {
        throw new IllegalArgumentException("unexpected backend config: " + key);
      }
    }
  }

  private static void validatePorts(String nodeId, Object portsValue) {
    if (!(portsValue instanceof List<?> ports)) {
      throw new IllegalArgumentException("node `" + nodeId + "` ports must be a list");
    }
    List<String> names = new ArrayList<>();
    for (Object value : ports) {
      if (!(value instanceof Map<?, ?> port)) {
        throw new IllegalArgumentException("node `" + nodeId + "` port must be an object");
      }
      String name = String.valueOf(port.get("name"));
      if (name.isBlank() || names.contains(name)) {
        throw new IllegalArgumentException("duplicate or missing port `" + name + "` on node `" + nodeId + "`");
      }
      names.add(name);
      Object accessValue = port.containsKey("access") ? port.get("access") : "read";
      String access = String.valueOf(accessValue);
      if (!VALID_ACCESS.contains(access)) {
        throw new IllegalArgumentException("unsupported access mode: " + access);
      }
      Object residencyValue = port.get("residency");
      if (residencyValue != null && !VALID_RESIDENCY.contains(String.valueOf(residencyValue))) {
        throw new IllegalArgumentException("unsupported residency: " + residencyValue);
      }
      if (port.get("layout") != null && residencyValue == null) {
        throw new IllegalArgumentException("layout requires residency");
      }
    }
  }

  private static void validateBoundaryContracts(Object contractsValue) {
    if (contractsValue == null) {
      return;
    }
    if (!(contractsValue instanceof List<?> contracts)) {
      throw new IllegalArgumentException("boundary_contracts must be a list");
    }
    for (Object value : contracts) {
      if (!(value instanceof Map<?, ?> contract)) {
        throw new IllegalArgumentException("boundary contract must be an object");
      }
      if (String.valueOf(contract.get("type_key")).isBlank()) {
        throw new IllegalArgumentException("boundary contract type_key must not be empty");
      }
      Object capabilitiesValue = contract.get("capabilities");
      if (!(capabilitiesValue instanceof Map<?, ?> capabilities)) {
        throw new IllegalArgumentException("boundary contract capabilities must be an object");
      }
      for (Object capability : capabilities.keySet()) {
        String name = String.valueOf(capability);
        if (!List.of("owned_move", "shared_clone", "borrow_ref", "borrow_mut", "metadata_read", "metadata_write", "backing_read", "backing_write").contains(name)) {
          throw new IllegalArgumentException("unsupported boundary capability: " + name);
        }
      }
    }
  }

  private List<Method> nodeMethods() {
    List<Method> nodes = new ArrayList<>();
    for (Method method : pluginClass.getDeclaredMethods()) {
      if (method.isAnnotationPresent(Node.class)) {
        nodes.add(method);
      }
    }
    nodes.sort(Comparator.comparing(method -> method.getAnnotation(Node.class).id()));
    return nodes;
  }

  private Map<String, Object> nodeMap(Method method) {
    Node node = method.getAnnotation(Node.class);
    Map<String, Object> metadata = new LinkedHashMap<>();
    if (!node.capability().isEmpty()) {
      metadata.put("capability", node.capability());
    }
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("id", node.id());
    map.put("backend", "java");
    map.put("entrypoint", method.getName());
    map.put("stateful", node.state() != Void.class);
    map.put("feature_flags", List.of());
    map.put("inputs", ports(node.inputs(), method.getParameters(), node));
    map.put("outputs", outputPorts(node.outputs(), node));
    map.put("metadata", metadata);
    return map;
  }

  private Map<String, Object> backendMap(Method method, Node node) {
    Map<String, Object> backend = new LinkedHashMap<>();
    backend.put("backend", "java");
    backend.put("runtime_model", "persistent_worker");
    backend.put("entry_class", pluginClass.getName());
    backend.put("entry_symbol", method.getName());
    backend.put("executable", "java");
    backend.put("args", List.of());
    backend.put("classpath", classesDirs);
    backend.put("native_library_paths", nativeLibraries);
    backend.put("env", Map.of());
    backend.put("options", Map.of("payload_transport", Map.of("direct_byte_buffer", true, "mmap", true)));
    return backend;
  }

  private List<Object> ports(String[] names, Parameter[] parameters, Node node) {
    List<Object> ports = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      Class<?> type = i < parameters.length ? parameters[i].getType() : Object.class;
      ports.add(port(names[i], type, node.access(), node.residency(), node.layout()));
    }
    return ports;
  }

  private List<Object> outputPorts(String[] names, Node node) {
    List<Object> ports = new ArrayList<>();
    for (String name : names) {
      ports.add(port(name, Object.class, "read", node.residency(), node.layout()));
    }
    return ports;
  }

  private Map<String, Object> port(
      String name, Class<?> type, String access, String residency, String layout) {
    Map<String, Object> port = new LinkedHashMap<>();
    port.put("name", name);
    port.put("ty", (name.contains("rgba") || name.equals("payload") || name.equals("frame") || name.equals("blob"))
        ? Map.of("Scalar", "Bytes")
        : typeExpr(type));
    port.put("optional", false);
    port.put("access", access);
    if (!residency.isEmpty()) {
      port.put("residency", residency);
    }
    if (!layout.isEmpty()) {
      port.put("layout", layout);
    }
    return port;
  }

  private Map<String, Object> typeExpr(Class<?> type) {
    if (type == boolean.class || type == Boolean.class) return Map.of("Scalar", "Bool");
    if (type == double.class || type == Double.class || type == float.class || type == Float.class) {
      return Map.of("Scalar", "Float");
    }
    if (type == String.class) return Map.of("Scalar", "String");
    if (type == Void.class) return Map.of("Scalar", "Unit");
    if (Number.class.isAssignableFrom(type) || type.isPrimitive()) return Map.of("Scalar", "Int");
    TypeKey key = type.getAnnotation(TypeKey.class);
    return Map.of("Opaque", key == null ? type.getSimpleName() : key.value());
  }

  private List<Object> boundaryContracts(BoundaryContract[] contracts) {
    List<Object> items = new ArrayList<>();
    for (BoundaryContract contract : contracts) {
      if (contract.typeKey().isBlank()) {
        throw new IllegalArgumentException("boundary contract type_key must not be empty");
      }
      List<String> declaredCapabilities = Arrays.asList(contract.capabilities());
      for (String capability : declaredCapabilities) {
        if (!VALID_BOUNDARY_CAPABILITIES.contains(capability)) {
          throw new IllegalArgumentException("unsupported boundary capability: " + capability);
        }
      }
      boolean hostRead = List.of(contract.capabilities()).contains("host_read");
      boolean workerWrite = List.of(contract.capabilities()).contains("worker_write");
      Map<String, Object> capabilities = new LinkedHashMap<>();
      capabilities.put("owned_move", true);
      capabilities.put("shared_clone", hostRead);
      capabilities.put("borrow_ref", hostRead);
      capabilities.put("borrow_mut", workerWrite);
      capabilities.put("metadata_read", hostRead);
      capabilities.put("metadata_write", workerWrite);
      capabilities.put("backing_read", hostRead);
      capabilities.put("backing_write", workerWrite);
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("type_key", contract.typeKey());
      item.put("rust_type_name", null);
      item.put("abi_version", 1);
      item.put("layout_hash", contract.typeKey());
      item.put("capabilities", capabilities);
      items.add(item);
    }
    return items;
  }

  private List<Object> artifacts() {
    List<Object> artifacts = new ArrayList<>();
    for (String classesDir : classesDirs) artifacts.add(artifact(classesDir, "classes_dir"));
    for (String jar : jars) artifacts.add(artifact(jar, "jar"));
    for (String nativeLibrary : nativeLibraries) artifacts.add(artifact(nativeLibrary, "native_library"));
    return artifacts;
  }

  private Map<String, Object> artifact(String path, String kind) {
    Map<String, Object> artifact = new LinkedHashMap<>();
    artifact.put("path", path);
    artifact.put("kind", kind);
    artifact.put("backend", "java");
    artifact.put("platform", null);
    artifact.put("sha256", null);
    artifact.put("metadata", Map.of());
    return artifact;
  }

  private Map<String, Object> metadata() {
    List<String> adapterIds = new ArrayList<>();
    for (Method method : pluginClass.getDeclaredMethods()) {
      Adapter adapter = method.getAnnotation(Adapter.class);
      if (adapter != null) {
        adapterIds.add(adapter.id());
      }
    }
    return Map.of(
        "language", "java",
        "package_builder", "dev.daedalus.plugin",
        "adapters", adapterIds);
  }
}
