package daedalus.examples;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.Plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Emit a manifest that exercises structured (non-image) types across the subprocess boundary. */
public final class EmitManifestStructuredDemo {
  private EmitManifestStructuredDemo() {}

  public static void main(String[] args) throws Exception {
    Path out;
    if (args.length > 0) {
      out = Paths.get(args[0]).toAbsolutePath();
    } else {
      Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
      out = tmp.resolve("demo_java_struct_" + System.nanoTime() + ".manifest.json").toAbsolutePath();
    }
    Files.createDirectories(out.getParent());

    Plugin plugin = new Plugin("demo_java_struct");
    plugin.version = "0.1.0";
    plugin.description = "Structured demo";

    AnnotationEmitter.registerAnnotated(plugin, ".", JavaStructuredDemoNodes.class);
    plugin.emitManifest(out);
    System.out.println(out.toAbsolutePath());
  }
}
