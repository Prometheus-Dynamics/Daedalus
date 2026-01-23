#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import ts from "typescript";

function die(msg) {
  process.stderr.write(String(msg) + "\n");
  process.exit(2);
}

function parseArgs(argv) {
  const out = {
    project: "tsconfig.json",
    outManifest: "daedalus.manifest.json",
    emitDir: null,
    pluginName: null,
    pluginVersion: null,
    pluginDescription: null,
    pluginMetadata: {},
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = argv[i + 1];
    if (a === "--project" && next) out.project = next, i++;
    else if (a === "--out" && next) out.outManifest = next, i++;
    else if (a === "--emit-dir" && next) out.emitDir = next, i++;
    else if (a === "--plugin-name" && next) out.pluginName = next, i++;
    else if (a === "--plugin-version" && next) out.pluginVersion = next, i++;
    else if (a === "--plugin-description" && next) out.pluginDescription = next, i++;
    else if (a === "--plugin-metadata" && next) out.pluginMetadata = JSON.parse(next), i++;
    else if (a === "--help" || a === "-h") {
      process.stdout.write(
        [
          "daedalus-node-tsc: emit a Daedalus manifest from TypeScript decorators.",
          "",
          "Usage:",
          "  daedalus-node-tsc --project tsconfig.json --out ./dist/plugin.manifest.json \\",
          "    [--emit-dir ./dist] \\",
          "    --plugin-name my_plugin [--plugin-version 0.1.1] [--plugin-description ...]",
          "",
          "Notes:",
          "- Finds @nodeMethod(...) and @node(...) decorators in your TS sources.",
          "- Infers port types from TS types for basic scalars/structs/optionals/lists/tuples.",
          "- For ambiguous unions/enums, supply explicit `input_types`/`output_types` in the decorator args.",
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

function readTsConfig(projectPath) {
  const configFile = ts.readConfigFile(projectPath, ts.sys.readFile);
  if (configFile.error) {
    die(ts.formatDiagnosticsWithColorAndContext([configFile.error], {
      getCanonicalFileName: (f) => f,
      getCurrentDirectory: () => process.cwd(),
      getNewLine: () => "\n",
    }));
  }
  const basePath = path.dirname(projectPath);
  const parsed = ts.parseJsonConfigFileContent(
    configFile.config,
    ts.sys,
    basePath,
    undefined,
    projectPath,
  );
  if (parsed.errors && parsed.errors.length) {
    die(ts.formatDiagnosticsWithColorAndContext(parsed.errors, {
      getCanonicalFileName: (f) => f,
      getCurrentDirectory: () => process.cwd(),
      getNewLine: () => "\n",
    }));
  }
  return parsed;
}

function isCallToIdentifier(expr, name) {
  if (!expr || !ts.isCallExpression(expr)) return false;
  const callee = expr.expression;
  return ts.isIdentifier(callee) && callee.text === name;
}

function evalLiteral(node) {
  if (!node) return undefined;
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text;
  if (ts.isNumericLiteral(node)) return Number(node.text);
  if (node.kind === ts.SyntaxKind.TrueKeyword) return true;
  if (node.kind === ts.SyntaxKind.FalseKeyword) return false;
  if (node.kind === ts.SyntaxKind.NullKeyword) return null;
  if (ts.isArrayLiteralExpression(node)) return node.elements.map(evalLiteral);
  if (ts.isObjectLiteralExpression(node)) {
    const out = {};
    for (const prop of node.properties) {
      if (!ts.isPropertyAssignment(prop)) continue;
      const key = ts.isIdentifier(prop.name)
        ? prop.name.text
        : ts.isStringLiteral(prop.name)
          ? prop.name.text
          : null;
      if (!key) continue;
      out[key] = evalLiteral(prop.initializer);
    }
    return out;
  }
  return undefined;
}

function tsTypeBrand(type, checker) {
  const sym = type.aliasSymbol ?? type.symbol;
  if (!sym) return null;
  const name = sym.getName();
  if (name === "Int" || name === "Float" || name === "Bool" || name === "Bytes") return name;
  return null;
}

function inferTypeExpr(type, checker, depth = 0) {
  if (depth > 8) return null;
  if (!type) return null;

  const brand = tsTypeBrand(type, checker);
  if (brand) return { Scalar: brand };

  const flags = type.getFlags();
  if (flags & ts.TypeFlags.StringLike) return { Scalar: "String" };
  if (flags & ts.TypeFlags.BooleanLike) return { Scalar: "Bool" };
  if (flags & ts.TypeFlags.BigIntLike) return { Scalar: "Int" };
  if (flags & ts.TypeFlags.NumberLike) return { Scalar: "Float" };

  if (flags & ts.TypeFlags.Undefined || flags & ts.TypeFlags.Null) return { Scalar: "Unit" };

  // Optional: T | null | undefined
  if (type.isUnion && type.isUnion()) {
    const parts = type.types;
    const nonNull = parts.filter((t) => {
      const f = t.getFlags();
      return !(f & ts.TypeFlags.Null) && !(f & ts.TypeFlags.Undefined);
    });
    if (nonNull.length === 1 && nonNull.length !== parts.length) {
      const inner = inferTypeExpr(nonNull[0], checker, depth + 1);
      return inner ? { Optional: inner } : null;
    }
    return null;
  }

  // Arrays: T[]
  if (checker.isArrayType(type)) {
    const args = checker.getTypeArguments(type);
    const inner = args && args.length ? inferTypeExpr(args[0], checker, depth + 1) : null;
    return inner ? { List: inner } : null;
  }

  // Map<K,V> => Map
  {
    const sym = type.getSymbol?.() ?? type.symbol;
    if (sym && sym.getName && sym.getName() === "Map") {
      const args = checker.getTypeArguments(type);
      if (args && args.length === 2) {
        const k = inferTypeExpr(args[0], checker, depth + 1);
        const v = inferTypeExpr(args[1], checker, depth + 1);
        if (k && v) return { Map: [k, v] };
      }
    }
  }

  // String index signature => Map<String, V>
  if (typeof type.getStringIndexType === "function") {
    const vTy = type.getStringIndexType();
    if (vTy) {
      const inner = inferTypeExpr(vTy, checker, depth + 1);
      return inner ? { Map: [{ Scalar: "String" }, inner] } : null;
    }
  }

  // Tuples
  if (checker.isTupleType(type)) {
    const args = checker.getTypeArguments(type);
    const items = args.map((t) => inferTypeExpr(t, checker, depth + 1));
    if (items.some((x) => !x)) return null;
    return { Tuple: items };
  }

  // Uint8Array / Buffer => Bytes
  const sym = type.getSymbol();
  if (sym && (sym.getName() === "Uint8Array" || sym.getName() === "Buffer")) {
    return { Scalar: "Bytes" };
  }

  // Struct-like object types: interface or type literal
  if (flags & ts.TypeFlags.Object) {
    const props = checker.getPropertiesOfType(type);
    if (!props || props.length === 0) return null;
    const fields = [];
    for (const p of props) {
      const decl = p.valueDeclaration || (p.declarations && p.declarations[0]) || null;
      if (!decl) return null;
      const pt = checker.getTypeOfSymbolAtLocation(p, decl);
      const ty = inferTypeExpr(pt, checker, depth + 1);
      if (!ty) return null;
      fields.push({ name: p.getName(), ty });
    }
    return { Struct: fields };
  }

  return null;
}

function inferPortsFromSignature(fn, checker) {
  const sig = checker.getSignatureFromDeclaration(fn);
  if (!sig) return { inputs: [], outputs: [] };
  const params = fn.parameters || [];
  const inputs = params.map((p) => {
    const name = ts.isIdentifier(p.name) ? p.name.text : null;
    if (!name) return null;
    const t = checker.getTypeAtLocation(p);
    const ty = inferTypeExpr(t, checker);
    if (!ty) return null;
    return { name, ty };
  });
  if (inputs.some((x) => !x)) return { inputs: [], outputs: [] };

  const ret = checker.getReturnTypeOfSignature(sig);
  if (checker.isTupleType(ret)) {
    const items = checker.getTypeArguments(ret).map((t) => inferTypeExpr(t, checker));
    if (items.some((x) => !x)) return { inputs: inputs.filter(Boolean), outputs: [] };
    const outs = items.map((ty, i) => ({ name: `out${i}`, ty }));
    return { inputs: inputs.filter(Boolean), outputs: outs };
  }
  const outTy = inferTypeExpr(ret, checker);
  if (!outTy) return { inputs: inputs.filter(Boolean), outputs: [] };
  return { inputs: inputs.filter(Boolean), outputs: [{ name: "out", ty: outTy }] };
}

function findDecoratorArgObject(decorators, which) {
  if (!decorators) return null;
  for (const d of decorators) {
    const e = d.expression;
    if (!isCallToIdentifier(e, which)) continue;
    const arg0 = e.arguments && e.arguments.length ? e.arguments[0] : null;
    if (!arg0 || !ts.isObjectLiteralExpression(arg0)) return null;
    return arg0;
  }
  return null;
}

function collectNodes(program, checker) {
  const nodes = [];
  for (const sf of program.getSourceFiles()) {
    if (sf.isDeclarationFile) continue;
    const filePath = sf.fileName;

    function visit(n) {
      // Class static method decorator.
      if (ts.isMethodDeclaration(n) && n.decorators && n.name && ts.isIdentifier(n.name)) {
        const argObj = findDecoratorArgObject(n.decorators, "nodeMethod");
        if (argObj) {
          const args = evalLiteral(argObj) || {};
          const classDecl = n.parent && ts.isClassDeclaration(n.parent) ? n.parent : null;
          const className = classDecl && classDecl.name ? classDecl.name.text : null;
          const jsFunction = className ? `${className}.${n.name.text}` : n.name.text;
          nodes.push({
            filePath,
            decl: n,
            decoratorArgs: args,
            jsFunction,
          });
        }
      }

      // (Mostly for JS authors): allow `@node(...)` on class methods too.
      if (ts.isMethodDeclaration(n) && n.decorators && n.name && ts.isIdentifier(n.name)) {
        const argObj = findDecoratorArgObject(n.decorators, "node");
        if (argObj) {
          const args = evalLiteral(argObj) || {};
          const classDecl = n.parent && ts.isClassDeclaration(n.parent) ? n.parent : null;
          const className = classDecl && classDecl.name ? classDecl.name.text : null;
          const jsFunction = className ? `${className}.${n.name.text}` : n.name.text;
          nodes.push({
            filePath,
            decl: n,
            decoratorArgs: args,
            jsFunction,
          });
        }
      }

      ts.forEachChild(n, visit);
    }

    visit(sf);
  }
  return nodes;
}

function main() {
  const args = parseArgs(process.argv);
  if (!args.pluginName) die("--plugin-name is required");

  const projectPath = path.resolve(args.project);
  const parsed = readTsConfig(projectPath);

  const compilerOptions = { ...parsed.options };
  const configBase = path.dirname(projectPath);
  const outDir = args.emitDir
    ? path.resolve(args.emitDir)
    : compilerOptions.outDir
      ? path.resolve(configBase, compilerOptions.outDir)
      : path.resolve(configBase, "dist");

  // Ensure ESM output by default (the bridge expects ESM-friendly modules).
  if (compilerOptions.module == null) compilerOptions.module = ts.ModuleKind.ESNext;
  if (compilerOptions.target == null) compilerOptions.target = ts.ScriptTarget.ES2022;
  compilerOptions.outDir = outDir;

  const program = ts.createProgram({
    rootNames: parsed.fileNames,
    options: compilerOptions,
  });

  const diagnostics = ts.getPreEmitDiagnostics(program);
  if (diagnostics.length) {
    die(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (f) => f,
      getCurrentDirectory: () => process.cwd(),
      getNewLine: () => "\n",
    }));
  }

  const emitRes = program.emit();
  if (emitRes.emitSkipped) {
    die("ts emit failed");
  }

  const commonSrcDir = path.resolve(program.getCommonSourceDirectory() || configBase);

  const checker = program.getTypeChecker();
  const discovered = collectNodes(program, checker);

  const outNodes = [];
  for (const n of discovered) {
    const dec = n.decoratorArgs || {};
    const id = dec.id;
    if (!id || typeof id !== "string") {
      die(`missing string 'id' in decorator args for ${n.filePath}`);
    }

    const inferred = inferPortsFromSignature(n.decl, checker);
    const inputs = Array.isArray(dec.inputs) ? dec.inputs : inferred.inputs;
    const outputs = Array.isArray(dec.outputs) ? dec.outputs : inferred.outputs;
    if (!inputs || inputs.length === 0) {
      die(`failed to infer inputs for ${id}; provide 'inputs' and 'input_types' explicitly`);
    }
    if (!outputs || outputs.length === 0) {
      die(`failed to infer outputs for ${id}; provide 'outputs' and 'output_types' explicitly`);
    }

    // If ports are given as names, fill with inferred types.
    const inputTypes = dec.input_types || {};
    const outputTypes = dec.output_types || {};
    const normalizePorts = (ports, inferredPorts, extraTypes) => {
      const inferredMap = new Map((inferredPorts || []).map((p) => [p.name, p.ty]));
      return ports.map((p) => {
        if (typeof p === "string") {
          const ty = extraTypes[p] || inferredMap.get(p) || null;
          if (!ty) die(`missing type for port '${p}' on node '${id}'`);
          return { name: p, ty };
        }
        if (p && typeof p === "object" && typeof p.name === "string" && p.ty) return p;
        die(`invalid port entry on node '${id}'`);
      });
    };

    const normalizedInputs = normalizePorts(inputs, inferred.inputs, inputTypes);
    const normalizedOutputs = normalizePorts(outputs, inferred.outputs, outputTypes);

    // Map TS source file path to emitted JS path under outDir (matching tsc's common source dir).
    const rel = path.relative(commonSrcDir, n.filePath);
    if (rel.startsWith("..")) {
      die(`internal: source file is outside common source dir (${commonSrcDir}): ${n.filePath}`);
    }
    const jsModule = path.resolve(outDir, rel.replace(/\.tsx?$/i, ".js"));

    outNodes.push({
      id,
      label: dec.label ?? null,
      js_path: jsModule,
      js_function: dec.func ?? n.jsFunction,
      stateful: !!dec.stateful,
      state: dec.state ?? null,
      capability: dec.capability ?? null,
      shader: dec.shader ?? null,
      feature_flags: Array.isArray(dec.feature_flags) ? dec.feature_flags : [],
      default_compute: dec.default_compute ?? "CpuOnly",
      sync_groups: Array.isArray(dec.sync_groups) ? dec.sync_groups : [],
      metadata: dec.metadata ?? {},
      inputs: normalizedInputs,
      outputs: normalizedOutputs,
    });
  }

  const doc = {
    manifest_version: "1",
    language: "node",
    plugin: {
      name: args.pluginName,
      version: args.pluginVersion,
      description: args.pluginDescription,
      metadata: args.pluginMetadata || {},
    },
    nodes: outNodes,
  };

  const outPath = path.resolve(args.outManifest);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(doc, null, 2), "utf8");
  process.stdout.write(outPath + "\n");
}

main();
