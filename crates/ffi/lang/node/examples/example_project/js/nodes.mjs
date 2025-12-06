// Daedalus Node nodes: minimal patterns.
//
// This file is meant to be copied into your own repo/project.
//
// Conventions:
// - Stateless nodes: positional args -> return value or array (multi-output).
// - Stateful nodes: a single invocation object -> return { state, outputs }.

export function add(a, b) {
  return Number(a) + Number(b);
}

export function split(value) {
  const v = Number(value);
  return [v, -v];
}

export function counter(inv) {
  const inc = Number(inv.args?.[0] ?? 0);
  const start = Number(inv.state_spec?.start ?? 0);
  const prev = inv.state?.value == null ? start : Number(inv.state.value);
  const next = prev + inc;
  return { state: { value: next }, outputs: { out: next } };
}

