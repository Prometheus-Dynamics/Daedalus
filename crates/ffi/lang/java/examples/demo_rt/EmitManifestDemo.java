package daedalus.examples;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.Plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class EmitManifestDemo {
  private EmitManifestDemo() {}

  public static void main(String[] args) throws Exception {
    Path out;
    if (args.length > 0) {
      out = Paths.get(args[0]).toAbsolutePath();
    } else {
      Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
      out = tmp.resolve("demo_java_rt_" + System.nanoTime() + ".manifest.json").toAbsolutePath();
    }
    Files.createDirectories(out.getParent());

    Plugin plugin = new Plugin("demo_java_rt");
    plugin.version = "1.0.0";
    plugin.description = "Demo Java nodes";
    plugin.metadata.put("author", "example");

    AnnotationEmitter.registerAnnotated(plugin, ".", JavaDemoNodes.class);

    plugin.emitManifest(out);
    System.out.println(out.toAbsolutePath());
  }
}
