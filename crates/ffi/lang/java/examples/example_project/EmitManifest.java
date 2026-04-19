package daedalus.example_project;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.NodeDef;
import daedalus.manifest.Plugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class EmitManifest {
  private EmitManifest() {}

  private static void copyWgsl(Path outDir) throws IOException {
    String fromEnv = System.getenv("DAEDALUS_WGSL_DIR");
    if (fromEnv == null || fromEnv.isEmpty()) return;

    Path src = Paths.get(fromEnv);
    if (!Files.isDirectory(src)) return;

    Path dst = outDir.resolve("shaders");
    Files.createDirectories(dst);
    try (var stream = Files.list(src)) {
      stream
          .filter(p -> p.toString().endsWith(".wgsl"))
          .forEach(
              p -> {
                try {
                  Files.copy(p, dst.resolve(p.getFileName().toString()));
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  public static void main(String[] args) throws Exception {
    Path out;
    if (args.length > 0) {
      out = Paths.get(args[0]).toAbsolutePath();
    } else {
      Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
      out = tmp.resolve("example_java_" + System.nanoTime() + ".manifest.json").toAbsolutePath();
    }
    Files.createDirectories(out.getParent());
    copyWgsl(out.getParent());

    Plugin plugin = new Plugin("example_java");
    plugin.version = "1.0.0";
    plugin.description = "Java example project";

    // Register nodes via annotations (Rust-like ergonomics).
    AnnotationEmitter.registerAnnotated(plugin, ".", ExampleNodes.class);

    // Optional: attach shader specs to a node. Keep it file-backed and relative to the manifest dir:
    // for (NodeDef n : plugin.getNodes()) {
    //   if (n.getId().equals("example_java:invert_first_u32")) {
    //     n.setShaderJson(
    //         """
    //         {"src_path":"shaders/invert.wgsl","entry":"main","name":"invert","invocations":[1,1,1],
    //          "bindings":[{"binding":0,"kind":"storage_buffer","access":"read_write","readback":true,"to_port":"out","size_bytes":4}]}
    //         """);
    //   }
    // }

    plugin.emitManifest(out);
    System.out.println(out.toString());
  }
}
