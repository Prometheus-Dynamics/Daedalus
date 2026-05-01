import { writeFile } from "node:fs/promises";

const SCHEMA_VERSION = 1;
const VALID_ACCESS = new Set(["read", "view", "modify", "move"]);
const VALID_RESIDENCY = new Set(["cpu", "gpu"]);
const VALID_BOUNDARY_CAPABILITIES = new Set(["host_read", "worker_write", "borrow_ref", "borrow_mut", "shared_clone"]);

function scalar(name) {
  return { Scalar: name };
}

function typeExpr(value) {
  if (typeof value === "object" && value?.__typeExpr) return value.__typeExpr;
  if (typeof value !== "string") return { Opaque: value?.key ?? "unknown" };
  if (value.startsWith("optional<")) return { Optional: typeExpr(value.slice(9, -1)) };
  if (value.startsWith("list<")) return { List: typeExpr(value.slice(5, -1)) };
  if (value.startsWith("map<")) {
    const inner = value.slice(4, -1);
    const [key, item] = inner.split(",");
    if (!key || !item) throw new TypeError(`invalid map type expression: ${value}`);
    return { Map: [typeExpr(key.trim()), typeExpr(item.trim())] };
  }
  if (value.startsWith("tuple<")) {
    return { Tuple: value.slice(6, -1).split(",").map((item) => typeExpr(item.trim())) };
  }
  switch (value) {
    case "unit": return scalar("Unit");
    case "bool": return scalar("Bool");
    case "i64": return scalar("Int");
    case "u64": return scalar("Int");
    case "f64": return scalar("Float");
    case "string": return scalar("String");
    case "bytes": return scalar("Bytes");
    default:
      throw new TypeError(`unsupported type expression: ${value}`);
  }
}

function port(name, spec) {
  const access = spec?.access ?? "read";
  validatePortShape(name, access, spec?.residency, spec?.layout);
  const out = {
    name,
    ty: typeExpr(spec),
    optional: false,
    access,
  };
  if (spec?.key) out.type_key = spec.key;
  if (spec?.residency) out.residency = spec.residency;
  if (spec?.layout) out.layout = spec.layout;
  return out;
}

export function config(name, fields) {
  return { key: name, fields, __typeExpr: { Opaque: name } };
}

config.port = (type, options = {}) => ({ type, options });

export function state(name, fields, init) {
  return { key: name, fields, init, __typeExpr: { Opaque: name } };
}

export function typeKey(key, fields) {
  return { key, fields, __typeExpr: { Opaque: key } };
}

typeKey.enum = (key, variants) => ({ key, variants, __typeExpr: { Opaque: key } });

export function adapter(id) {
  const decl = { id, sourceType: null, targetType: null, fn: null };
  return {
    source(sourceType) {
      decl.sourceType = sourceType;
      return this;
    },
    target(targetType) {
      decl.targetType = targetType;
      return this;
    },
    run(fn) {
      decl.fn = fn;
      return decl;
    },
  };
}

class NodeBuilder {
  constructor(id) {
    this.id = id;
    this.inputSpecs = {};
    this.outputSpecs = {};
    this.stateSpec = null;
    this.capabilityName = null;
    this.fn = null;
  }

  capability(name) {
    this.capabilityName = name;
    return this;
  }

  state(spec) {
    this.stateSpec = spec;
    return this;
  }

  inputs(specs) {
    this.inputSpecs = specs;
    return this;
  }

  outputs(specs) {
    this.outputSpecs = specs;
    return this;
  }

  run(fn) {
    this.fn = fn;
    return this;
  }

  schema() {
    if (Object.keys(this.inputSpecs).length === 0 && Object.keys(this.outputSpecs).length === 0) {
      throw new TypeError(`node \`${this.id}\` must declare inputs or outputs`);
    }
    return {
      id: this.id,
      backend: "node",
      entrypoint: this.id,
      stateful: this.stateSpec !== null,
      feature_flags: [],
      inputs: Object.entries(this.inputSpecs).map(([name, spec]) => port(name, spec)),
      outputs: Object.entries(this.outputSpecs).map(([name, spec]) => port(name, spec)),
      metadata: Object.fromEntries(Object.entries({ capability: this.capabilityName }).filter(([, value]) => value !== null)),
    };
  }
}

export function node(id) {
  return new NodeBuilder(id);
}

class PluginBuilder {
  constructor(name, nodes) {
    this.name = name;
    this.nodes = nodes;
    this.adapters = [];
    this.artifacts = [];
    this.boundaryContracts = [];
    this.transportOptions = {};
  }

  typeContract(typeKeyValue, capabilities) {
    validateTypeContract(typeKeyValue, capabilities);
    const hostRead = capabilities.includes("host_read");
    const workerWrite = capabilities.includes("worker_write");
    this.boundaryContracts.push({
      type_key: typeKeyValue,
      rust_type_name: null,
      abi_version: 1,
      layout_hash: typeKeyValue,
      capabilities: {
        owned_move: true,
        shared_clone: hostRead,
        borrow_ref: hostRead,
        borrow_mut: workerWrite,
        metadata_read: hostRead,
        metadata_write: workerWrite,
        backing_read: hostRead,
        backing_write: workerWrite,
      },
    });
    return this;
  }

  adapter(adapterDecl) {
    this.adapters.push(adapterDecl);
    return this;
  }

  transport(options) {
    this.transportOptions = { ...this.transportOptions, ...options };
    return this;
  }

  artifact(path) {
    this.artifacts.push(path);
    return this;
  }

  descriptor() {
    const nodes = this.nodes.map((nodeDecl) => nodeDecl.schema());
    const backends = Object.fromEntries(nodes.map((nodeDecl) => [nodeDecl.id, {
      backend: "node",
      runtime_model: "persistent_worker",
      entry_module: "src/plugin.ts",
      entry_symbol: nodeDecl.entrypoint,
      executable: "node",
      args: [],
      classpath: [],
      native_library_paths: [],
      env: {},
      options: { payload_transport: this.transportOptions },
    }]));
    const descriptor = {
      schema_version: SCHEMA_VERSION,
      schema: {
        schema_version: SCHEMA_VERSION,
        plugin: { name: this.name, version: "1.0.0", description: null, metadata: {} },
        dependencies: [],
        required_host_capabilities: [],
        feature_flags: [],
        boundary_contracts: this.boundaryContracts,
        nodes,
      },
      backends,
      artifacts: this.artifacts.map((path) => ({
        path,
        kind: "source_file",
        backend: "node",
        platform: null,
        sha256: null,
        metadata: {},
      })),
      lockfile: "plugin.lock.json",
      manifest_hash: null,
      signature: null,
      metadata: {
        language: "node",
        package_builder: "@daedalus/ffi-plugin",
        adapters: this.adapters.map((adapterDecl) => adapterDecl.id),
      },
    };
    validateDescriptor(descriptor);
    return descriptor;
  }

  async write(path) {
    await writeFile(path, `${JSON.stringify(this.descriptor(), null, 2)}\n`, "utf8");
  }
}

export function plugin(name, nodes) {
  return new PluginBuilder(name, nodes);
}

export function validateDescriptor(descriptor) {
  if (descriptor.schema_version !== SCHEMA_VERSION) throw new Error("unsupported schema_version");
  if (!descriptor.schema || !Array.isArray(descriptor.schema.nodes)) throw new Error("descriptor is missing schema nodes");
  if (!descriptor.backends || typeof descriptor.backends !== "object") throw new Error("descriptor is missing backends");
  const nodeIds = new Set();
  for (const nodeDecl of descriptor.schema.nodes) {
    if (!nodeDecl.id || nodeIds.has(nodeDecl.id)) throw new Error(`duplicate or missing node id: ${nodeDecl.id}`);
    nodeIds.add(nodeDecl.id);
    if (nodeDecl.backend !== "node") throw new Error(`node \`${nodeDecl.id}\` must use node backend`);
    for (const portDecl of [...(nodeDecl.inputs ?? []), ...(nodeDecl.outputs ?? [])]) {
      validatePortShape(portDecl.name, portDecl.access ?? "read", portDecl.residency, portDecl.layout);
    }
    const backend = descriptor.backends[nodeDecl.id];
    if (!backend) throw new Error(`node \`${nodeDecl.id}\` is missing backend config`);
    if (backend.runtime_model !== "persistent_worker") throw new Error(`node \`${nodeDecl.id}\` must use persistent_worker`);
    if (backend.backend !== "node") throw new Error(`node \`${nodeDecl.id}\` backend config must use node`);
  }
  const extraBackends = Object.keys(descriptor.backends).filter((nodeId) => !nodeIds.has(nodeId));
  if (extraBackends.length > 0) throw new Error(`unexpected backend configs: ${extraBackends.join(",")}`);
}

function validateTypeContract(typeKeyValue, capabilities) {
  if (!typeKeyValue) throw new Error("boundary contract type_key must not be empty");
  const invalid = capabilities.filter((capability) => !VALID_BOUNDARY_CAPABILITIES.has(capability));
  if (invalid.length > 0) throw new Error(`unsupported boundary capabilities: ${invalid.join(",")}`);
}

function validatePortShape(name, access, residency, layout) {
  if (!name) throw new Error("port name must not be empty");
  if (!VALID_ACCESS.has(access)) throw new Error(`unsupported access mode: ${access}`);
  if (residency !== undefined && !VALID_RESIDENCY.has(residency)) throw new Error(`unsupported residency: ${residency}`);
  if (layout !== undefined && residency === undefined) throw new Error("layout requires residency");
}

export const bytes = {
  view: () => ({ __typeExpr: scalar("Bytes"), access: "read" }),
  buffer: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "read", sharedMemory: options.sharedMemory ?? false }),
  sharedBuffer: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "read", sharedMemory: true }),
  cowBuffer: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "modify", cow: true }),
};

export const image = {
  rgba8: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "read", residency: options.residency, layout: options.layout }),
  mutableRgba8: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "modify", residency: options.residency, layout: options.layout }),
};

export const gpu = {
  rgba8: (options = {}) => ({ __typeExpr: scalar("Bytes"), access: options.access ?? "read", residency: options.residency ?? "gpu", layout: options.layout }),
};

export const event = {
  typedError(code, message) {
    const error = new Error(message);
    error.code = code;
    return error;
  },
};
