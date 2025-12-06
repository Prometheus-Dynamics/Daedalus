export function translate_point(pt, dx = 1, dy = -1) {
  return { x: Number(pt.x) + Number(dx), y: Number(pt.y) + Number(dy) };
}

export function flip_mode(mode) {
  const name = mode && mode.name;
  if (name === "A") return { name: "B", value: { x: 7, y: 9 } };
  return { name: "A", value: 1 };
}

export function map_len(m) {
  return Object.keys(m ?? {}).length;
}

export function list_sum(items) {
  return (items ?? []).reduce((a, b) => a + Number(b), 0);
}

