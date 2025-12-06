#!/usr/bin/env node
import process from "node:process";
import path from "node:path";
import { Plugin } from "../index.mjs";

function die(msg) {
  process.stderr.write(String(msg) + "\n");
  process.exit(2);
}

function parseArgs(argv) {
  const out = {
    project: "tsconfig.json",
    emitDir: null,
    pluginName: null,
    pluginVersion: null,
    pluginDescription: null,
    pluginMetadata: {},
    outName: "generated_node_plugin",
    manifestPath: null,
    build: true,
    bundle: true,
    bundleDeps: true,
    install: true,
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--project" && next) out.project = next, i++;
    else if (a === "--emit-dir" && next) out.emitDir = next, i++;
    else if (a === "--plugin-name" && next) out.pluginName = next, i++;
    else if (a === "--plugin-version" && next) out.pluginVersion = next, i++;
    else if (a === "--plugin-description" && next) out.pluginDescription = next, i++;
    else if (a === "--plugin-metadata" && next) out.pluginMetadata = JSON.parse(next), i++;
    else if (a === "--out-name" && next) out.outName = next, i++;
    else if (a === "--manifest" && next) out.manifestPath = next, i++;
    else if (a === "--build") out.build = true;
    else if (a === "--no-build") out.build = false;
    else if (a === "--bundle") out.bundle = true;
    else if (a === "--no-bundle") out.bundle = false;
    else if (a === "--bundle-deps") out.bundleDeps = true;
    else if (a === "--no-bundle-deps") out.bundleDeps = false;
    else if (a === "--install") out.install = true;
    else if (a === "--no-install") out.install = false;
    else if (a === "--help" || a === "-h") {
      process.stdout.write(
        [
          "daedalus-node-ts-pack: build TS, emit manifest, and bundle into a Rust plugin.",
          "",
          "Usage:",
          "  daedalus-node-ts-pack --project tsconfig.json --plugin-name my_plugin \\",
          "    [--emit-dir ./dist] [--manifest ./dist/my_plugin.manifest.json] \\",
          "    [--out-name my_plugin_bundle] [--no-build] [--no-bundle] [--no-bundle-deps]",
          "",
          "Notes:",
          "- Runs the same TS compiler-based emitter as daedalus-node-tsc.",
          "- By default bundles emitted JS (and shader files referenced by src_path) into `crates/ffi/examples/<out-name>_bundle/`.",
          "- Run from anywhere inside the Daedalus repo; it searches upward for `Cargo.lock`.",
          "",
        ].join("\n"),
      );
      process.exit(0);
    } else {
      die(`unknown arg: ${a}`);
    }
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv);
  if (!args.pluginName) die("--plugin-name is required");

  const plugin = new Plugin({
    name: args.pluginName,
    version: args.pluginVersion ?? null,
    description: args.pluginDescription ?? null,
    metadata: args.pluginMetadata ?? {},
  });

  const artifact = plugin.packTs({
    project: args.project,
    emit_dir: args.emitDir ? path.resolve(args.emitDir) : null,
    out_name: args.outName,
    manifest_path: args.manifestPath ? path.resolve(args.manifestPath) : null,
    build: args.build,
    bundle: args.bundle,
    bundle_deps: args.bundleDeps,
    install: args.install,
  });

  process.stdout.write(String(artifact) + "\n");
}

main();

