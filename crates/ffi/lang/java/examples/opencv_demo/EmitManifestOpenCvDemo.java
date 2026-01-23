package daedalus.examples;

import daedalus.annotations.AnnotationEmitter;
import daedalus.manifest.Plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Emit a manifest that demonstrates passing an image-like payload into Java, processing it with
 * OpenCV, and returning it back to Rust.
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>Requires OpenCV Java bindings (`org.opencv.*`) at runtime.
 *   <li>The image carrier is a struct `{data_b64,width,height,channels,dtype,layout,encoding}` where
 *       `data_b64` carries bytes; prefer `encoding=="raw"` (fast, no codec step).
 * </ul>
 */
public final class EmitManifestOpenCvDemo {
  private EmitManifestOpenCvDemo() {}

  public static void main(String[] args) throws Exception {
    Path out;
    if (args.length > 0) {
      out = Paths.get(args[0]).toAbsolutePath();
    } else {
      Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
      out = tmp.resolve("demo_java_opencv_" + System.nanoTime() + ".manifest.json").toAbsolutePath();
    }
    Files.createDirectories(out.getParent());

    Plugin plugin = new Plugin("demo_java_opencv");
    plugin.version = "0.1.1";
    plugin.description = "OpenCV image demo";

    AnnotationEmitter.registerAnnotated(plugin, ".", JavaOpenCvDemoNodes.class);
    plugin.emitManifest(out);
    System.out.println(out.toAbsolutePath());
  }
}
