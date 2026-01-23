package daedalus.example_project;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.Plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class BuildPlugin {
  private BuildPlugin() {}

  public static void main(String[] args) throws Exception {
    Path out = Paths.get(args.length > 0 ? args[0] : "example_java.so").toAbsolutePath();
    Files.createDirectories(out.getParent());

    Plugin plugin = new Plugin("example_java");
    plugin.version = "0.1.1";
    plugin.description = "Java example project";

    AnnotationEmitter.registerAnnotated(plugin, ".", ExampleNodes.class);

    // Build a bundled Rust plugin library; manifest is an internal implementation detail here.
    plugin.build(out, "example_java", true, true, false);
    System.out.println(out.toString());
  }
}
