export function add_defaults(a, b) {
  return a + b;
}

export function split(value) {
  return [value, -value];
}

export function scale_cfg(value, cfg) {
  return value * Number(cfg && cfg.factor != null ? cfg.factor : 1);
}

export function make_point(x, y) {
  return { x, y };
}

export function enum_mode(value) {
  if (value >= 0) return { name: "A", value: 1 };
  return { name: "B", value: { x: 7, y: 9 } };
}

export function sync_a_only(a, _b) {
  return a;
}

export function sync_a_only_obj(a, _b) {
  return a;
}

export function ctx_echo(text, extra) {
  const nid = extra && extra.node ? extra.node.id : null;
  return `${text}|${nid}`;
}

export function choose_mode_meta(mode) {
  return `mode=${mode}`;
}

export function shader_invert(img) {
  return img;
}

export function shader_write_u32() {
  return [];
}

export function shader_counter() {
  return [];
}

export function shader_counter_cpu() {
  return [];
}

export function shader_counter_gpu() {
  return [];
}

export function shader_multi_write(_which) {
  return [];
}

export function multi_emit(extra) {
  const io = extra && extra.io ? extra.io : null;
  if (!io) return 0;
  io.push("out", 1);
  io.push("out", 2);
  return 0;
}

export function accum({ args, state }) {
  const v = Number(args && args.length ? args[0] : 0);
  const st = state && typeof state === "object" ? state : { total: 10 };
  const next = { total: Number(st.total || 0) + v };
  return [next, next.total];
}

