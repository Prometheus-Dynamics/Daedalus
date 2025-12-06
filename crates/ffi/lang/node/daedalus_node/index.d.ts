export type ScalarName = "Int" | "Float" | "Bool" | "String" | "Bytes" | "Unit";

export type TypeExpr =
  | { Scalar: ScalarName }
  | { Optional: TypeExpr }
  | { List: TypeExpr }
  | { Tuple: TypeExpr[] }
  | { Map: [TypeExpr, TypeExpr] }
  | { Struct: Array<{ name: string; ty: TypeExpr }> }
  | { Enum: Array<{ name: string; ty: TypeExpr | null }> };

export type Port = {
  name: string;
  ty: TypeExpr;
  source?: string;
  const_value?: unknown;
};

export const t: {
  int(): { Scalar: "Int" };
  float(): { Scalar: "Float" };
  bool(): { Scalar: "Bool" };
  string(): { Scalar: "String" };
  bytes(): { Scalar: "Bytes" };
  unit(): { Scalar: "Unit" };
  optional(inner: TypeExpr): { Optional: TypeExpr };
  list(inner: TypeExpr): { List: TypeExpr };
  tuple(items: TypeExpr[]): { Tuple: TypeExpr[] };
  map(k: TypeExpr, v: TypeExpr): { Map: [TypeExpr, TypeExpr] };
  struct(fields: Array<{ name: string; ty: TypeExpr }>): { Struct: Array<{ name: string; ty: TypeExpr }> };
  enum(variants: Array<{ name: string; ty?: TypeExpr | null }>): { Enum: Array<{ name: string; ty: TypeExpr | null }> };
};

export function port(
  name: string,
  ty: TypeExpr,
  opts?: { default?: unknown; source?: string; const_value?: unknown },
): Port;

export function inputs(...ports: Array<Port | string | [string, TypeExpr] | Array<Port | string | [string, TypeExpr]>>): Port[];
export function outputs(...ports: Array<Port | string | [string, TypeExpr] | Array<Port | string | [string, TypeExpr]>>): Port[];

export const SyncPolicy: Readonly<{
  AllReady: "AllReady";
  Latest: "Latest";
  ZipByTag: "ZipByTag";
}>;

export const BackpressureStrategy: Readonly<{
  None: "None";
  BoundedQueues: "BoundedQueues";
  ErrorOnOverflow: "ErrorOnOverflow";
}>;

export type SyncGroup = {
  ports: string[];
  name?: string;
  policy: typeof SyncPolicy[keyof typeof SyncPolicy] | string;
  backpressure?: typeof BackpressureStrategy[keyof typeof BackpressureStrategy] | string;
  capacity?: number;
};

export function syncGroup(args: {
  ports: string[];
  name?: string | null;
  policy?: SyncGroup["policy"] | null;
  backpressure?: SyncGroup["backpressure"] | null;
  capacity?: number | null;
}): SyncGroup;

export function wgsl(srcOrPath: string): string;

export type ShaderImage = {
  src?: string;
  src_path?: string;
  entry: string;
  name?: string;
  workgroup_size?: [number, number, number];
  shaders?: Array<{
    name: string;
    src?: string;
    src_path?: string;
    entry?: string;
    workgroup_size?: [number, number, number];
  }>;
  dispatch?: string;
  dispatch_from_port?: string;
  input_binding: number;
  output_binding: number;
  bindings?: Array<{
    binding: number;
    kind: "texture2d_rgba8" | "storage_texture2d_rgba8" | "uniform_buffer" | "storage_buffer";
    access: "read_only" | "write_only" | "read_write";
    readback?: boolean;
    state_backend?: "cpu" | "gpu";
    from_port?: string;
    from_state?: string;
    to_port?: string;
    to_state?: string;
    size_bytes?: number;
  }>;
  invocations?: [number, number, number];
};

export function shaderImage(
  srcOrPath: string,
  opts?: {
    entry?: string;
    name?: string | null;
    workgroup_size?: [number, number, number] | null;
    input_binding?: number;
    output_binding?: number;
  },
): ShaderImage;

export function shaderImagePath(
  path: string,
  opts?: {
    entry?: string;
    name?: string | null;
    workgroup_size?: [number, number, number] | null;
    input_binding?: number;
    output_binding?: number;
  },
): ShaderImage;

export class NodeDef {
  id: string;
  label: string | null;
  js_module: string | null;
  js_path: string | null;
  js_function: string | null;
  raw_io: boolean;
  stateful: boolean;
  state: unknown;
  capability: string | null;
  shader: ShaderImage | null;
  feature_flags: string[];
  default_compute: string;
  sync_groups: Array<string[] | SyncGroup>;
  metadata: Record<string, unknown>;
  inputs: Port[];
  outputs: Port[];

  constructor(args: {
    id: string;
    label?: string | null;
    js_module?: string | null;
    js_path?: string | null;
    js_function?: string | null;
    raw_io?: boolean;
    stateful?: boolean;
    state?: unknown;
    capability?: string | null;
    shader?: ShaderImage | null;
    feature_flags?: string[];
    default_compute?: string;
    sync_groups?: Array<string[] | SyncGroup>;
    metadata?: Record<string, unknown>;
    inputs?: Port[];
    outputs?: Port[];
  });

  toManifest(): unknown;
}

export class Plugin {
  name: string;
  version: string | null;
  description: string | null;
  metadata: Record<string, unknown>;
  nodes: NodeDef[];

  constructor(args: { name: string; version?: string | null; description?: string | null; metadata?: Record<string, unknown> });
  register(node: NodeDef): void;
  emitManifest(filePath: string): string;
  packFromManifest(args: { out_name?: string; manifest_path: string; build?: boolean; bundle?: boolean; bundle_deps?: boolean }): string;
  pack(args?: { out_name?: string; manifest_path?: string | null; build?: boolean; bundle?: boolean; bundle_deps?: boolean }): string;
  packTs(args?: {
    project?: string;
    emit_dir?: string | null;
    out_name?: string;
    manifest_path?: string | null;
    build?: boolean;
    bundle?: boolean;
    bundle_deps?: boolean;
    install?: boolean;
  }): string;
}

export function node(args: {
  id: string;
  label?: string;
  module?: string;
  path?: string;
  func?: string;
  inputs?: Array<Port | string | [string, TypeExpr]> | Record<string, TypeExpr>;
  outputs?: Array<Port | string | [string, TypeExpr]> | Record<string, TypeExpr>;
  defaults?: Record<string, unknown>;
  input_types?: Record<string, TypeExpr>;
  output_types?: Record<string, TypeExpr>;
  raw_io?: boolean;
  stateful?: boolean;
  state?: unknown;
  capability?: string | null;
  shader?: ShaderImage | null;
  feature_flags?: string[];
  default_compute?: string;
  sync_groups?: Array<string[] | SyncGroup>;
  metadata?: Record<string, unknown>;
}): <T extends Function>(fn: T) => T;

export function nodeMethod(args: Parameters<typeof node>[0]): (
  target: unknown,
  propertyKey: string,
  descriptor: PropertyDescriptor,
) => PropertyDescriptor;

// Scalar "brands" that the TS manifest emitter understands.
export type Int = number & { readonly __daedalus_scalar: "Int" };
export type Float = number & { readonly __daedalus_scalar: "Float" };
export type Bool = boolean & { readonly __daedalus_scalar: "Bool" };
export type Bytes = Uint8Array & { readonly __daedalus_scalar: "Bytes" };
