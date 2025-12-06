const vscode = require('vscode');
const cp = require('child_process');
const fs = require('fs/promises');
const path = require('path');

function activate(context) {
  const output = vscode.window.createOutputChannel('Helios SDK');
  context.subscriptions.push(output);

  context.subscriptions.push(vscode.commands.registerCommand('heliosSdk.buildPlugin', async () => {
    await runBuild(output, false);
  }));

  context.subscriptions.push(vscode.commands.registerCommand('heliosSdk.buildAndInstall', async () => {
    await runBuild(output, true);
  }));

  context.subscriptions.push(vscode.commands.registerCommand('heliosSdk.installPluginFromPath', async () => {
    await installFromPrompt(output);
  }));

  context.subscriptions.push(vscode.commands.registerCommand('heliosSdk.createPluginProject', async () => {
    await createPluginProject(output);
  }));
}

async function runBuild(output, shouldInstall) {
  const config = vscode.workspace.getConfiguration('heliosSdk');
  let buildCommand = config.get('buildCommand');
  if (!buildCommand) {
    buildCommand = await vscode.window.showInputBox({
      prompt: 'Build command',
      placeHolder: 'cargo build --release'
    });
  }
  if (!buildCommand) return;

  const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
  const workingDir = config.get('buildWorkingDirectory') || workspaceRoot;
  if (!workingDir) {
    vscode.window.showErrorMessage('No workspace folder is open.');
    return;
  }

  try {
    await runCommand(output, buildCommand, workingDir);
    vscode.window.showInformationMessage('Build finished.');
  } catch (err) {
    vscode.window.showErrorMessage(`Build failed: ${err.message || err}`);
    return;
  }

  if (!shouldInstall) return;
  const outputPath = await resolvePluginPath(config.get('pluginOutputPath'));
  if (!outputPath) return;
  await installPlugin(output, outputPath);
}

async function installFromPrompt(output) {
  const outputPath = await resolvePluginPath('');
  if (!outputPath) return;
  await installPlugin(output, outputPath);
}

async function resolvePluginPath(defaultValue) {
  const input = await vscode.window.showInputBox({
    prompt: 'Path to built .so plugin',
    value: defaultValue || '',
    placeHolder: '/var/lib/helios/sdk/default/artifacts/plugin.so'
  });
  if (!input) return null;
  if (!input.trim().endsWith('.so')) {
    vscode.window.showErrorMessage('Plugin path must point to a .so file.');
    return null;
  }
  return input.trim();
}

async function installPlugin(output, pluginPath) {
  const config = vscode.workspace.getConfiguration('heliosSdk');
  const base = (config.get('apiBase') || '').replace(/\/+$/, '');
  if (!base) {
    vscode.window.showErrorMessage('Set heliosSdk.apiBase to your Helios API URL.');
    return;
  }
  output.appendLine(`Installing plugin from ${pluginPath}...`);
  try {
    const response = await fetch(`${base}/plugins/install`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ source_path: pluginPath })
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(message || `Install failed (${response.status})`);
    }
    vscode.window.showInformationMessage('Plugin installed.');
  } catch (err) {
    vscode.window.showErrorMessage(`Install failed: ${err.message || err}`);
  }
}

function runCommand(output, command, cwd) {
  return new Promise((resolve, reject) => {
    output.appendLine(`$ ${command}`);
    const child = cp.spawn(command, { cwd, shell: true, env: process.env });
    child.stdout?.on('data', (data) => output.append(data.toString()));
    child.stderr?.on('data', (data) => output.append(data.toString()));
    child.on('error', (err) => reject(err));
    child.on('close', (code) => {
      if (code === 0) return resolve();
      return reject(new Error(`Command exited with code ${code}`));
    });
  });
}

async function createPluginProject(output) {
  const config = vscode.workspace.getConfiguration('heliosSdk');
  const sdkRoot = resolveSdkRoot(config);
  const projectsRoot = resolveProjectsRoot(config);
  if (!sdkRoot) {
    vscode.window.showErrorMessage('Unable to find IDE SDK root. Set heliosSdk.sdkRoot or ensure IDE_SDK_DIR is set.');
    return;
  }
  if (!projectsRoot) {
    vscode.window.showErrorMessage('Unable to find SDK projects directory. Set heliosSdk.projectsRoot or ensure IDE_PROJECTS_DIR is set.');
    return;
  }

  const language = await vscode.window.showQuickPick(
    [
      { label: 'Rust', value: 'rust' },
      { label: 'Node/TypeScript', value: 'node' },
      { label: 'Python', value: 'python' },
      { label: 'Java', value: 'java' },
      { label: 'C/C++', value: 'c_cpp' }
    ],
    { placeHolder: 'Select a Daedalus FFI template' }
  );
  if (!language) return;

  const projectName = await vscode.window.showInputBox({
    prompt: `New ${language.label} plugin project name`,
    placeHolder: 'example_plugin'
  });
  if (!projectName || !projectName.trim()) return;

  const destRoot = path.join(projectsRoot, sanitizeName(projectName));
  if (await pathExists(destRoot)) {
    vscode.window.showErrorMessage(`Project folder already exists: ${destRoot}`);
    return;
  }

  const daedalusRoot = path.join(sdkRoot, 'Daedalus');
  const templatePath = resolveTemplatePath(daedalusRoot, language.value);
  if (!templatePath) {
    vscode.window.showErrorMessage(`Template not available for ${language.label}.`);
    return;
  }
  if (!(await pathExists(templatePath))) {
    vscode.window.showErrorMessage(`Template folder missing: ${templatePath}`);
    return;
  }

  await copyDir(templatePath, destRoot);
  await addLanguageSupportFiles(daedalusRoot, destRoot, language.value);
  await patchTemplateFiles(daedalusRoot, destRoot, language.value, output);

  vscode.window.showInformationMessage(`Created ${language.label} plugin project.`);
  await vscode.commands.executeCommand('vscode.openFolder', vscode.Uri.file(destRoot), true);
}

function resolveSdkRoot(config) {
  const override = (config.get('sdkRoot') || '').trim();
  if (override) return override;
  return process.env.IDE_SDK_DIR || '';
}

function resolveProjectsRoot(config) {
  const override = (config.get('projectsRoot') || '').trim();
  if (override) return override;
  return process.env.IDE_PROJECTS_DIR || '';
}

function sanitizeName(name) {
  return name.trim().replace(/[^\w.-]+/g, '_');
}

function resolveTemplatePath(daedalusRoot, language) {
  const base = path.join(daedalusRoot, 'crates', 'ffi', 'lang');
  switch (language) {
    case 'rust':
      return path.join(base, 'rust', 'example_project');
    case 'node':
      return path.join(base, 'node', 'example_project');
    case 'python':
      return path.join(base, 'python', 'examples');
    case 'java':
      return path.join(base, 'java', 'examples');
    case 'c_cpp':
      return path.join(base, 'c_cpp', 'example_project');
    default:
      return null;
  }
}

async function addLanguageSupportFiles(daedalusRoot, destRoot, language) {
  const base = path.join(daedalusRoot, 'crates', 'ffi', 'lang');
  if (language === 'node') {
    await copyDir(path.join(base, 'node', 'daedalus_node'), path.join(destRoot, 'daedalus_node'));
  }
  if (language === 'python') {
    await copyDir(path.join(base, 'python', 'daedalus_py'), path.join(destRoot, 'daedalus_py'));
  }
  if (language === 'java') {
    await copyDir(path.join(base, 'java', 'sdk'), path.join(destRoot, 'sdk'));
  }
  if (language === 'c_cpp') {
    await copyDir(path.join(base, 'c_cpp', 'sdk'), path.join(destRoot, 'sdk'));
  }
}

async function patchTemplateFiles(daedalusRoot, destRoot, language, output) {
  if (language === 'rust') {
    const cargoPath = path.join(destRoot, 'Cargo.toml');
    if (await pathExists(cargoPath)) {
      const daedalusCrate = path.join(daedalusRoot, 'crates', 'daedalus');
      if (await pathExists(daedalusCrate)) {
        const content = await fs.readFile(cargoPath, 'utf8');
        const updated = content.replace(
          /daedalus\s*=\s*\{\s*path\s*=\s*"[^"]+"/,
          `daedalus = { path = "${daedalusCrate.replace(/\\/g, '/')}"`
        );
        await fs.writeFile(cargoPath, updated);
      } else {
        output.appendLine(`Daedalus crate not found at ${daedalusCrate}; leaving Cargo.toml unchanged.`);
      }
    }
  }

  if (language === 'node') {
    const tsNodes = path.join(destRoot, 'ts', 'nodes.ts');
    if (await pathExists(tsNodes)) {
      const content = await fs.readFile(tsNodes, 'utf8');
      const updated = content.replace('../../daedalus_node/index.js', '../daedalus_node/index.js');
      await fs.writeFile(tsNodes, updated);
    }
    const jsEmit = path.join(destRoot, 'js', 'emit_manifest.mjs');
    if (await pathExists(jsEmit)) {
      const content = await fs.readFile(jsEmit, 'utf8');
      const updated = content.replace(
        /const repoRoot[\s\S]+?await import\(`file:\/\/\$\{sdk\}`\);/,
        [
          'const sdk = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../daedalus_node/index.mjs");',
          'const { Plugin, NodeDef, t } = await import(`file://${sdk}`);'
        ].join('\n')
      );
      await fs.writeFile(jsEmit, updated);
    }
  }

  if (language === 'c_cpp') {
    const buildPath = path.join(destRoot, 'build.sh');
    if (await pathExists(buildPath)) {
      const script = [
        '#!/usr/bin/env bash',
        'set -euo pipefail',
        '',
        'OUT_DIR="${1:-/tmp/example_cpp}"',
        'mkdir -p "$OUT_DIR"',
        '',
        'ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"',
        '',
        'SRC="$ROOT/nodes.cpp"',
        'HDR="$ROOT/sdk/daedalus_c_cpp.h"',
        'SHADERS_DIR="$ROOT/shaders"',
        '',
        'OS="$(uname -s | tr \'[:upper:]\' \'[:lower:]\')"',
        'LIB_EXT="so"',
        'if [[ "$OS" == "darwin" ]]; then',
        '  LIB_EXT="dylib"',
        'elif [[ "$OS" == "mingw"* || "$OS" == "msys"* || "$OS" == "cygwin"* ]]; then',
        '  LIB_EXT="dll"',
        'fi',
        '',
        'LIB="$OUT_DIR/libexample_cpp_nodes.$LIB_EXT"',
        '',
        'echo "[c_cpp] building $LIB"',
        'c++ -std=c++17 -O2 -fPIC -shared -I"$ROOT/sdk" "$SRC" -o "$LIB"',
        '',
        'MANIFEST="$OUT_DIR/example_cpp.manifest.json"',
        'export LIB',
        'export MANIFEST',
        '',
        '# Copy shader assets next to the manifest/library so `src_path` resolves.',
        'mkdir -p "$OUT_DIR/shaders"',
        'cp -f "$SHADERS_DIR/"*.wgsl "$OUT_DIR/shaders/" 2>/dev/null || true',
        '',
        '# Emit a manifest file by calling the dylib\'s exported `daedalus_cpp_manifest` symbol, then',
        '# patch in cc_path to point back at this dylib (the "manifest file" flow).',
        'python - <<\'PY\'',
        'import ctypes',
        'import json',
        'import os',
        'from pathlib import Path',
        '',
        'lib_path = Path(os.environ["LIB"]).resolve()',
        'out = Path(os.environ["MANIFEST"]).resolve()',
        'out.parent.mkdir(parents=True, exist_ok=True)',
        '',
        'class Result(ctypes.Structure):',
        '    _fields_ = [("json", ctypes.c_char_p), ("error", ctypes.c_char_p)]',
        '',
        'lib = ctypes.CDLL(str(lib_path))',
        'mf = lib.daedalus_cpp_manifest',
        'mf.restype = Result',
        'free = lib.daedalus_free',
        'free.argtypes = [ctypes.c_void_p]',
        '',
        'res = mf()',
        'if res.error:',
        '    err = ctypes.string_at(res.error).decode("utf-8", errors="replace")',
        '    free(res.error)',
        '    raise SystemExit(err)',
        'if not res.json:',
        '    raise SystemExit("daedalus_cpp_manifest returned null")',
        '',
        'json_str = ctypes.string_at(res.json).decode("utf-8", errors="replace")',
        'free(res.json)',
        '',
        'doc = json.loads(json_str)',
        'doc["language"] = "c_cpp"',
        'for n in doc.get("nodes", []):',
        '    n.setdefault("cc_path", lib_path.name)',
        '    n.setdefault("cc_free", "daedalus_free")',
        '',
        'out.write_text(json.dumps(doc, indent=2) + "\\n", encoding="utf-8")',
        'print(out.as_posix())',
        'PY',
        '',
        'echo "[c_cpp] wrote $MANIFEST (and $LIB exports daedalus_cpp_manifest for manifest-less loading)"'
      ].join('\n');
      await fs.writeFile(buildPath, script, { mode: 0o755 });
    }
  }
}

async function copyDir(src, dest) {
  await fs.mkdir(dest, { recursive: true });
  const entries = await fs.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      await copyDir(srcPath, destPath);
    } else if (entry.isFile()) {
      await fs.copyFile(srcPath, destPath);
    }
  }
}

async function pathExists(target) {
  try {
    await fs.access(target);
    return true;
  } catch {
    return false;
  }
}

function deactivate() {}

module.exports = {
  activate,
  deactivate
};
