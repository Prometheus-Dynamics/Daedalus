package daedalus.examples;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.Plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Build a bundled Rust plugin library (`.so/.dylib/.dll`) for the OpenCV demo.
 *
 * <p>This requires OpenCV Java bindings on the classpath when compiling/running this tool.
 */
public final class BuildPluginOpenCvDemo {
  private BuildPluginOpenCvDemo() {}

  public static void main(String[] args) throws Exception {
    Path out = Paths.get(args.length > 0 ? args[0] : "demo_java_opencv.so").toAbsolutePath();
    Files.createDirectories(out.getParent());

    Plugin plugin = new Plugin("demo_java_opencv");
    plugin.version = "1.0.0";
    plugin.description = "OpenCV image demo";

    AnnotationEmitter.registerAnnotated(plugin, ".", JavaOpenCvDemoNodes.class);

    // Build a bundled Rust plugin library; manifest is an internal implementation detail.
    plugin.build(out, "demo_java_opencv", true, true, false);
    System.out.println(out.toString());
  }
}

