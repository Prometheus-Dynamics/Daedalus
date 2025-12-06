package daedalus.manifest;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public final class Plugin {
  public final String name;
  public String version = null;
  public String description = null;
  public Map<String, Object> metadata = new LinkedHashMap<>();
  public final List<NodeDef> nodes = new ArrayList<>();

  public Plugin(String name) {
    this.name = name;
  }

  public Plugin register(NodeDef node) {
    nodes.add(node);
    return this;
  }

  public Path emitManifest(Path path) throws IOException {
    Map<String, Object> doc = buildDoc();
    Files.createDirectories(path.toAbsolutePath().getParent());
    Files.write(path.toAbsolutePath(), Json.stringify(doc).getBytes(StandardCharsets.UTF_8));
    return path.toAbsolutePath();
  }

  public Path pack(String out_name, Path manifest_path, boolean build, boolean bundle) throws Exception {
    return pack(out_name, manifest_path, build, bundle, true);
  }

  public Path pack(String out_name, Path manifest_path, boolean build, boolean bundle, boolean lock)
      throws Exception {
    return pack(out_name, manifest_path, build, bundle, lock, false);
  }

  public Path pack(
      String out_name, Path manifest_path, boolean build, boolean bundle, boolean lock, boolean release)
      throws Exception {
    Path workspace = findWorkspaceRoot();
    Path ffiRoot = workspace.resolve("crates").resolve("ffi");
    Path examplesDir = ffiRoot.resolve("examples");
    Files.createDirectories(examplesDir);

    Path manifestAbs = emitManifest(manifest_path);
    Path baseDir = manifestAbs.getParent();

    Map<String, Object> doc = buildDoc();
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> nodeDocs = (List<Map<String, Object>>) doc.get("nodes");

    // Decorate with manifest hash/signature, and optionally write a lockfile.
    byte[] rawBytes = Files.readAllBytes(manifestAbs);
    String digest = sha256Hex(rawBytes);
    doc.put("manifest_hash", digest);
    doc.put("signature", null);

    Path lockPath = null;
    if (lock) {
      lockPath = manifestAbs.resolveSibling(manifestAbs.getFileName().toString() + ".lock");
      Files.write(lockPath, buildLockfile(nodeDocs).getBytes(StandardCharsets.UTF_8));
      doc.put("lockfile", baseDir.relativize(lockPath).toString().replace('\\', '/'));
    }

    Path manifestIncludePath = manifestAbs;
    List<BundleEntry> bundleEntries = new ArrayList<>();

    if (bundle) {
      Path bundleDir = examplesDir.resolve(out_name + "_bundle");
      Files.createDirectories(bundleDir);

      // 1) Bundle Java classpath directories/jars into `_bundle/java/*`.
      int cpIdx = 0;
      for (Map<String, Object> n : nodeDocs) {
        Object cpObj = n.get("java_classpath");
        if (!(cpObj instanceof String)) continue;
        String cp = (String) cpObj;
        if (cp.isEmpty()) continue;

        Path abs = toAbs(baseDir, cp);
        if (!Files.exists(abs)) continue;

        String rel;
        if (Files.isDirectory(abs)) {
          rel = Paths.get("_bundle", "java", "cp" + (cpIdx++)).toString().replace('\\', '/');
          Path dest = bundleDir.resolve(rel);
          copyTree(abs, dest);
          addBundleEntries(bundleDir, dest, bundleEntries);
        } else {
          String fname = abs.getFileName().toString();
          rel = Paths.get("_bundle", "java", "cp" + (cpIdx++), fname).toString().replace('\\', '/');
          Path dest = bundleDir.resolve(rel);
          Files.createDirectories(dest.getParent());
          Files.copy(abs, dest, StandardCopyOption.REPLACE_EXISTING);
          bundleEntries.add(new BundleEntry(rel, dest.toAbsolutePath()));
        }
        n.put("java_classpath", rel);
      }

      // 2) Bundle WGSL files referenced via `src_path`.
      int[] extIdx = new int[] {0};
      for (Map<String, Object> n : nodeDocs) {
        Object shaderObj = n.get("shader");
        if (!(shaderObj instanceof Map)) continue;
        @SuppressWarnings("unchecked")
        Map<String, Object> shader = (Map<String, Object>) shaderObj;

        bundleShaderPath(shader, "src_path", baseDir, bundleDir, bundleEntries, extIdx);
        Object shadersObj = shader.get("shaders");
        if (shadersObj instanceof List) {
          @SuppressWarnings("unchecked")
          List<Object> shaders = (List<Object>) shadersObj;
          for (Object sObj : shaders) {
            if (!(sObj instanceof Map)) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> s = (Map<String, Object>) sObj;
            bundleShaderPath(s, "src_path", baseDir, bundleDir, bundleEntries, extIdx);
          }
        }
      }

      // 3) Bundle lockfile if present.
      if (lockPath != null && Files.exists(lockPath)) {
        String rel = baseDir.relativize(lockPath).toString().replace('\\', '/');
        Path dest = bundleDir.resolve(rel);
        Files.createDirectories(dest.getParent());
        Files.copy(lockPath, dest, StandardCopyOption.REPLACE_EXISTING);
        bundleEntries.add(new BundleEntry(rel, dest.toAbsolutePath()));
        doc.put("lockfile", rel);
      }

      Path bundledManifest = bundleDir.resolve("manifest.json");
      Files.write(bundledManifest, Json.stringify(doc).getBytes(StandardCharsets.UTF_8));
      manifestIncludePath = bundledManifest;
    }

    Path examplePath = examplesDir.resolve(out_name + ".rs");
    Files.write(
        examplePath, rustWrapper(out_name, manifestIncludePath, baseDir, bundle, bundleEntries).getBytes(StandardCharsets.UTF_8));

    // For parity with Node/Python pack APIs: return where the artifact would be built.
    String profile = release ? "release" : System.getenv().getOrDefault("PROFILE", "debug");
    String prefix = isWindows() ? "" : "lib";
    String ext = isWindows() ? ".dll" : (isMac() ? ".dylib" : ".so");
    Path artifact = workspace.resolve("target").resolve(profile).resolve("examples").resolve(prefix + out_name + ext);

    if (build) {
      List<String> cmd = new ArrayList<>();
      cmd.add("cargo");
      cmd.add("build");
      cmd.add("-p");
      cmd.add("daedalus-ffi");
      cmd.add("--example");
      cmd.add(out_name);
      if (release) cmd.add("--release");
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.directory(workspace.toFile());
      pb.inheritIO();
      Process p = pb.start();
      int code = p.waitFor();
      if (code != 0) throw new RuntimeException("cargo build failed (status=" + code + ")");
      if (!Files.exists(artifact)) {
        throw new RuntimeException("expected artifact missing at " + artifact.toAbsolutePath());
      }
    }

    return artifact;
  }

  public Path build(Path outPath, String outName) throws Exception {
    return build(outPath, outName, true, true, false);
  }

  public Path build(
      Path outPath, String outName, boolean bundle, boolean release, boolean keepIntermediates)
      throws Exception {
    Path tmp = Files.createTempDirectory("daedalus_java_build_" + name + "_");
    Path manifestTmp = tmp.resolve(name + ".manifest.json");
    Path artifact = pack(outName, manifestTmp, true, bundle, false, release);
    Files.createDirectories(outPath.toAbsolutePath().getParent());
    Files.copy(artifact, outPath.toAbsolutePath(), StandardCopyOption.REPLACE_EXISTING);

    if (!keepIntermediates) {
      Path workspace = findWorkspaceRoot();
      Path examplesDir = workspace.resolve("crates").resolve("ffi").resolve("examples");
      try {
        Files.deleteIfExists(examplesDir.resolve(outName + ".rs"));
      } catch (Exception ignored) {}
      try {
        deleteTree(examplesDir.resolve(outName + "_bundle"));
      } catch (Exception ignored) {}
      try {
        deleteTree(tmp);
      } catch (Exception ignored) {}
    }

    return outPath.toAbsolutePath();
  }

  private Map<String, Object> buildDoc() {
    Map<String, Object> doc = new LinkedHashMap<>();
    doc.put("manifest_version", "1");
    doc.put("language", "java");
    Map<String, Object> plugin = new LinkedHashMap<>();
    plugin.put("name", name);
    if (version != null) plugin.put("version", version);
    if (description != null) plugin.put("description", description);
    if (metadata != null && !metadata.isEmpty()) plugin.put("metadata", metadata);
    doc.put("plugin", plugin);
    List<Object> ns = new ArrayList<>();
    for (NodeDef n : nodes) ns.add(n.toManifest());
    doc.put("nodes", ns);
    return doc;
  }

  private static String sha256Hex(byte[] bytes) throws Exception {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] out = md.digest(bytes);
    StringBuilder sb = new StringBuilder();
    for (byte b : out) sb.append(String.format("%02x", b));
    return sb.toString();
  }

  private static String buildLockfile(List<Map<String, Object>> nodeDocs) {
    StringBuilder sb = new StringBuilder();
    sb.append("java_lock_v1\n");
    sb.append("java=").append(runQuiet(new String[] {"java", "-version"})).append("\n");
    sb.append("javac=").append(runQuiet(new String[] {"javac", "--version"})).append("\n");
    sb.append("nodes=").append(nodeDocs.size()).append("\n");
    for (Map<String, Object> n : nodeDocs) {
      Object id = n.get("id");
      Object cp = n.get("java_classpath");
      Object cls = n.get("java_class");
      Object method = n.get("java_method");
      sb.append("- id=").append(id).append(" cp=").append(cp).append(" class=").append(cls).append(" method=").append(method).append("\n");
    }
    return sb.toString();
  }

  private static String runQuiet(String[] cmd) {
    try {
      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      try (InputStream in = p.getInputStream()) {
        byte[] buf = in.readAllBytes();
        p.waitFor();
        String s = new String(buf, StandardCharsets.UTF_8).trim();
        return s.replace("\n", "\\n");
      }
    } catch (Exception ex) {
      return "unavailable";
    }
  }

  private static void bundleShaderPath(
      Map<String, Object> obj,
      String key,
      Path baseDir,
      Path bundleDir,
      List<BundleEntry> entries,
      int[] extIdx) throws IOException {
    Object p = obj.get(key);
    if (!(p instanceof String)) return;
    String sp = (String) p;
    if (sp.isEmpty()) return;
    Path abs = toAbs(baseDir, sp);
    if (!Files.exists(abs) || !Files.isRegularFile(abs)) return;

    String rel;
    try {
      rel = baseDir.relativize(abs).toString().replace('\\', '/');
    } catch (Exception ex) {
      rel = Paths.get("_external", String.valueOf(extIdx[0]++), abs.getFileName().toString())
          .toString()
          .replace('\\', '/');
    }
    Path dest = bundleDir.resolve(rel);
    Files.createDirectories(dest.getParent());
    Files.copy(abs, dest, StandardCopyOption.REPLACE_EXISTING);
    entries.add(new BundleEntry(rel, dest.toAbsolutePath()));
    obj.put(key, rel);
  }

  private static Path toAbs(Path baseDir, String p) {
    Path fp = Paths.get(p);
    if (fp.isAbsolute()) return fp;
    return baseDir.resolve(fp).normalize();
  }

  private static void copyTree(Path from, Path to) throws IOException {
    Files.walkFileTree(
        from,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Path rel = from.relativize(dir);
            Files.createDirectories(to.resolve(rel));
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Path rel = from.relativize(file);
            Path dest = to.resolve(rel);
            Files.createDirectories(dest.getParent());
            Files.copy(file, dest, StandardCopyOption.REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void deleteTree(Path root) throws IOException {
    if (!Files.exists(root)) return;
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void addBundleEntries(Path bundleDir, Path root, List<BundleEntry> entries)
      throws IOException {
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            String rel = bundleDir.relativize(file).toString().replace('\\', '/');
            entries.add(new BundleEntry(rel, file.toAbsolutePath()));
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static String rustWrapper(
      String outName,
      Path manifestIncludePath,
      Path baseDir,
      boolean bundle,
      List<BundleEntry> entries) {
    String manifestStr = manifestIncludePath.toString().replace("\\", "\\\\");
    String baseStr = baseDir.toString().replace("\\", "\\\\");

    StringBuilder bundleCode = new StringBuilder();
    if (bundle) {
      bundleCode.append(
          "\nfn extract_bundle() -> std::path::PathBuf {\n"
              + "    let nanos = std::time::SystemTime::now()\n"
              + "        .duration_since(std::time::UNIX_EPOCH)\n"
              + "        .unwrap()\n"
              + "        .as_nanos();\n"
              + "    let dir = std::env::temp_dir().join(format!(\"daedalus_java_bundle_" + outName + "_{}_{}\", std::process::id(), nanos));\n"
              + "    std::fs::create_dir_all(&dir).expect(\"create bundle temp dir\");\n"
              + "    for (rel, bytes) in BUNDLE_FILES {\n"
              + "        let dest = dir.join(rel);\n"
              + "        if let Some(parent) = dest.parent() {\n"
              + "            let _ = std::fs::create_dir_all(parent);\n"
              + "        }\n"
              + "        std::fs::write(&dest, bytes).expect(\"write bundled file\");\n"
              + "    }\n"
              + "    dir\n"
              + "}\n\n"
              + "static BUNDLE_FILES: &[(&str, &[u8])] = &[\n");
      for (BundleEntry e : entries) {
        String abs = e.abs.toString().replace("\\", "\\\\");
        bundleCode
            .append("    (")
            .append(quote(e.rel))
            .append(", include_bytes!(r#\"")
            .append(abs)
            .append("\"#) as &[u8]),\n");
      }
      bundleCode.append("];\n");
    }

    String useBundle = bundle ? "true" : "false";
    return ""
        + "#![crate_type = \"cdylib\"]\n"
        + "use daedalus_ffi::export_plugin;\n"
        + "use daedalus_ffi::{JavaManifest, JavaManifestPlugin};\n"
        + "use daedalus_runtime::plugins::{Plugin, PluginRegistry};\n"
        + "use serde_json;\n\n"
        + "static MANIFEST_JSON: &str = include_str!(r#\"" + manifestStr + "\"#);\n"
        + "static BASE_DIR: &str = r#\"" + baseStr + "\"#;\n"
        + bundleCode
        + "\n"
        + "pub struct GeneratedJavaPlugin {\n"
        + "    inner: JavaManifestPlugin,\n"
        + "}\n\n"
        + "impl Default for GeneratedJavaPlugin {\n"
        + "    fn default() -> Self {\n"
        + "        let manifest: JavaManifest = serde_json::from_str(MANIFEST_JSON).expect(\"invalid embedded manifest\");\n"
        + "        let base = if " + useBundle + " { extract_bundle() } else { std::path::PathBuf::from(BASE_DIR) };\n"
        + "        Self { inner: JavaManifestPlugin::from_manifest_with_base(manifest, Some(base)) }\n"
        + "    }\n"
        + "}\n\n"
        + "impl Plugin for GeneratedJavaPlugin {\n"
        + "    fn id(&self) -> &'static str { self.inner.id() }\n"
        + "    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> { self.inner.install(registry) }\n"
        + "}\n\n"
        + "export_plugin!(GeneratedJavaPlugin);\n";
  }

  private static String quote(String s) {
    String esc = s.replace("\\", "\\\\").replace("\"", "\\\"");
    return "\"" + esc + "\"";
  }

  private static boolean isWindows() {
    String os = System.getProperty("os.name", "").toLowerCase();
    return os.contains("win");
  }

  private static boolean isMac() {
    String os = System.getProperty("os.name", "").toLowerCase();
    return os.contains("mac");
  }

  private static Path findWorkspaceRoot() {
    String env = System.getenv("DAEDALUS_WORKSPACE_ROOT");
    if (env != null && !env.isEmpty()) {
      return Paths.get(env).toAbsolutePath();
    }
    Path cur = Paths.get("").toAbsolutePath();
    for (int i = 0; i < 12; i++) {
      if (Files.exists(cur.resolve("Cargo.lock"))) return cur;
      Path parent = cur.getParent();
      if (parent == null) break;
      cur = parent;
    }
    // As a fallback, try from the classpath location of this class.
    try {
      Path here =
          Paths.get(Plugin.class.getProtectionDomain().getCodeSource().getLocation().toURI())
              .toAbsolutePath();
      Path p = Files.isDirectory(here) ? here : here.getParent();
      if (p != null) {
        for (int i = 0; i < 12; i++) {
          if (Files.exists(p.resolve("Cargo.lock"))) return p;
          Path parent = p.getParent();
          if (parent == null) break;
          p = parent;
        }
      }
    } catch (Exception ignored) {
    }
    throw new IllegalStateException("failed to find workspace root (set DAEDALUS_WORKSPACE_ROOT)");
  }

  private static final class BundleEntry {
    public final String rel;
    public final Path abs;

    public BundleEntry(String rel, Path abs) {
      this.rel = rel;
      this.abs = abs;
    }
  }
}
