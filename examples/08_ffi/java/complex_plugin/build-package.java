import dev.daedalus.plugin.PackageBuilder;
import ffi.showcase.ShowcasePlugin;

final class BuildPackage {
  public static void main(String[] args) throws Exception {
    PackageBuilder.fromAnnotatedPlugin(ShowcasePlugin.class)
        .classesDir("build/classes/java/main")
        .jar("build/libs/ffi-showcase.jar")
        .nativeLibrary("build/native/libffi_showcase_jni.so")
        .write("plugin.json");
  }
}
