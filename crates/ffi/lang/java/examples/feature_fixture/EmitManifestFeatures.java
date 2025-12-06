package daedalus.examples;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.BackpressureStrategy;
import daedalus.manifest.ManifestBuilders;
import daedalus.manifest.NodeDef;
import daedalus.manifest.Plugin;
import daedalus.manifest.SyncPolicy;
import daedalus.manifest.Types;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class EmitManifestFeatures {
  private EmitManifestFeatures() {}

  public static void main(String[] args) throws Exception {
    Path out;
    if (args.length > 0) {
      out = Paths.get(args[0]).toAbsolutePath();
    } else {
      Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
      out = tmp.resolve("demo_java_feat_" + System.nanoTime() + ".manifest.json").toAbsolutePath();
    }
    Path outDir = out.toAbsolutePath().getParent();
    Files.createDirectories(outDir);

    // Copy shared WGSL fixtures alongside the manifest so all shaders use `src_path`.
    Path wgslDir = resolveWgslDir();
    for (String name :
        Arrays.asList("invert.wgsl", "write_u32.wgsl", "counter.wgsl", "write_one.wgsl", "write_two.wgsl")) {
      Files.copy(
          wgslDir.resolve(name), outDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
    }

    String prefix = "demo_java_feat";
    Plugin plugin = new Plugin(prefix);
    plugin.version = "0.1.0";
    plugin.description = "Java feature fixture";
    plugin.metadata.put("category", "tests");

    // Most nodes are defined "Rust-style" via Java annotations on the functions.
    AnnotationEmitter.registerAnnotated(plugin, ".", JavaFeaturesNodes.class);

    // Capability dispatch (Rust side) stays as a tiny explicit node (no Java entrypoint needed).
    NodeDef capAdd = new NodeDef(prefix + ":cap_add");
    capAdd.label = "CapAdd";
    capAdd.capability = "Add";
    capAdd.metadata.put("category", "capability");
    capAdd.input(ManifestBuilders.port("a", Types.intTy()));
    capAdd.input(ManifestBuilders.port("b", Types.intTy()));
    capAdd.output(ManifestBuilders.port("out", Types.intTy()));
    plugin.register(capAdd);

    // GPU-required placeholder (planner gating).
    NodeDef gpuReq =
        new NodeDef(prefix + ":gpu_required_placeholder")
            .javaEntrypoint(".", "daedalus.examples.JavaFeaturesNodes", "add_defaults");
    gpuReq.label = "GpuRequiredPlaceholder";
    gpuReq.default_compute = "GpuRequired";
    gpuReq.metadata.put("category", "gpu");
    gpuReq.input(ManifestBuilders.port("x", Types.intTy(), 1, null));
    gpuReq.output(ManifestBuilders.port("out", Types.intTy()));
    plugin.register(gpuReq);

    // Shader nodes are executed by Rust, but live in the language manifest.
    plugin.register(shaderInvert(prefix, "invert.wgsl"));
    plugin.register(shaderWriteU32(prefix, "write_u32.wgsl"));
    plugin.register(shaderCounter(prefix, "counter.wgsl", "counter", null));
    plugin.register(shaderCounter(prefix, "counter.wgsl", "counter_cpu", null));
    plugin.register(shaderCounter(prefix, "counter.wgsl", "counter_gpu", "gpu"));
    plugin.register(shaderMultiWrite(prefix, "write_one.wgsl", "write_two.wgsl"));

    plugin.emitManifest(out);
    System.out.println(out.toAbsolutePath());
  }

  private static Path resolveWgslDir() {
    String fromEnv = System.getenv("DAEDALUS_WGSL_DIR");
    if (fromEnv != null && !fromEnv.isEmpty()) {
      return Paths.get(fromEnv);
    }
    // Fallback for manual runs from repo root.
    return Paths.get("crates", "ffi", "lang", "shaders");
  }

  private static NodeDef shaderInvert(String prefix, String srcPath) {
    NodeDef n =
        new NodeDef(prefix + ":shader_invert")
            .javaEntrypoint(".", "daedalus.examples.JavaFeaturesNodes", "add_defaults");
    n.label = "ShaderInvert";
    n.metadata.put("category", "gpu");
    n.shader = ManifestBuilders.shaderImagePath(srcPath, "invert");
    n.inputs.add(ManifestBuilders.port("img", JavaFeatureTypes.imageTy()));
    n.outputs.add(ManifestBuilders.port("img", JavaFeatureTypes.imageTy()));
    return n;
  }

  private static NodeDef shaderWriteU32(String prefix, String srcPath) {
    NodeDef n =
        new NodeDef(prefix + ":shader_write_u32")
            .javaEntrypoint(".", "daedalus.examples.JavaFeaturesNodes", "add_defaults");
    n.label = "ShaderWriteU32";
    n.metadata.put("category", "gpu");
    n.shader = shaderBufferWrite(srcPath, "write_u32", null, null, null);
    n.outputs.add(ManifestBuilders.port("out", Types.bytesTy()));
    return n;
  }

  private static NodeDef shaderCounter(String prefix, String srcPath, String name, String stateBackend) {
    NodeDef n =
        new NodeDef(prefix + ":" + name)
            .javaEntrypoint(".", "daedalus.examples.JavaFeaturesNodes", "add_defaults");
    n.label = "ShaderCounter";
    n.metadata.put("category", "gpu");
    n.shader = shaderBufferWrite(srcPath, name, "counter", "counter", stateBackend);
    n.outputs.add(ManifestBuilders.port("out", Types.bytesTy()));
    return n;
  }

  private static Object shaderBufferWrite(
      String srcPath, String name, String fromState, String toState, String stateBackend) {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("src_path", srcPath);
    s.put("entry", "main");
    s.put("name", name);
    s.put("invocations", new Object[] {1, 1, 1});
    List<Object> bindings = new ArrayList<>();
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("binding", 0);
    b.put("kind", "storage_buffer");
    b.put("access", "read_write");
    b.put("readback", true);
    b.put("to_port", "out");
    b.put("size_bytes", 4);
    if (fromState != null) b.put("from_state", fromState);
    if (toState != null) b.put("to_state", toState);
    if (stateBackend != null) b.put("state_backend", stateBackend);
    bindings.add(b);
    s.put("bindings", bindings);
    return s;
  }

  private static NodeDef shaderMultiWrite(String prefix, String one, String two) {
    NodeDef n =
        new NodeDef(prefix + ":shader_multi_write")
            .javaEntrypoint(".", "daedalus.examples.JavaFeaturesNodes", "add_defaults");
    n.label = "ShaderMultiWrite";
    n.metadata.put("category", "gpu");
    Map<String, Object> s = new LinkedHashMap<>();
    List<Object> shaders = new ArrayList<>();
    Map<String, Object> s1 = new LinkedHashMap<>();
    s1.put("name", "one");
    s1.put("src_path", one);
    s1.put("entry", "main");
    Map<String, Object> s2 = new LinkedHashMap<>();
    s2.put("name", "two");
    s2.put("src_path", two);
    s2.put("entry", "main");
    shaders.add(s1);
    shaders.add(s2);
    s.put("shaders", shaders);
    s.put("dispatch_from_port", "which");
    s.put("invocations", new Object[] {1, 1, 1});
    List<Object> bindings = new ArrayList<>();
    Map<String, Object> b = new LinkedHashMap<>();
    b.put("binding", 0);
    b.put("kind", "storage_buffer");
    b.put("access", "read_write");
    b.put("readback", true);
    b.put("to_port", "out");
    b.put("size_bytes", 4);
    bindings.add(b);
    s.put("bindings", bindings);
    n.shader = s;

    n.inputs.add(ManifestBuilders.port("which", Types.stringTy()));
    n.outputs.add(ManifestBuilders.port("out", Types.bytesTy()));
    return n;
  }

  private static Map<String, Object> imageTy() {
    return JavaFeatureTypes.imageTy();
  }
}
