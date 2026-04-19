// Daedalus C++ nodes (Rust-like authoring):
//
// - Declare typed functions (C++ signature) and let the SDK infer `TypeExpr` in the manifest.
// - Register nodes with a single macro that:
//   - exports a stable C ABI symbol
//   - parses args from the Daedalus payload JSON
//   - calls your typed function
//   - returns JSON outputs
// - Export a `daedalus_cpp_manifest()` symbol so Rust can load this dylib directly
//   (single-artifact flow, close to Rust cdylib plugins).

#include "../sdk/daedalus.hpp"

#include <cstdint>
#include <tuple>

static int32_t add_i32(int32_t a, int32_t b) {
  return (int32_t)(a + b);
}

static std::tuple<int32_t, int32_t> split_i32(int32_t value) {
  return {value, (int32_t)(-value)};
}

DAEDALUS_STRUCT(CounterSpec, (start, int64_t, 0));
DAEDALUS_STRUCT(CounterState, (value, int64_t, 0));

DAEDALUS_STRUCT(Point, (x, int32_t, 0), (y, int32_t, 0));
DAEDALUS_ENUM(Mode, (A, int32_t), (B, Point));

static daedalus::StatefulResult<int32_t> counter_i32(const daedalus::StatefulContext& ctx, int32_t inc) {
  const int64_t start = ctx
                            .state_spec<CounterSpec>()
                            .value_or(CounterSpec{})
                            .start;
  const int64_t prev = ctx.state<CounterState>().value_or(CounterState{}).value;
  const int64_t next = prev + (int64_t)inc;
  daedalus::StatefulResult<int32_t> r;
  CounterState st;
  st.value = next;
  r.state_json = daedalus::Codec<CounterState>::encode(st);
  r.outputs = (int32_t)next;
  return r;
}

// Typed config struct (Rust NodeConfig-style, for basic cases).
DAEDALUS_STRUCT(ScaleCfg, (factor, int32_t, 2));

static int32_t scale_cfg_i32(int32_t value, ScaleCfg cfg) {
  return (int32_t)(value * cfg.factor);
}

static Mode enum_mode_i32(int32_t value) {
  if (value >= 0) return Mode::A(1);
  Point p;
  p.x = 7;
  p.y = 9;
  return Mode::B(p);
}

static int32_t sync_a_only_i32(int32_t a, std::optional<int32_t> _b) {
  return a;
}

// Register nodes (exported symbols + manifest entries).
DAEDALUS_NODE("example_cpp:add", add_i32, DAEDALUS_PORTS(a, b), DAEDALUS_PORTS(out))
DAEDALUS_NODE("example_cpp:split", split_i32, DAEDALUS_PORTS(value), DAEDALUS_PORTS(out0, out1))
DAEDALUS_NODE_WITH("example_cpp:enum_mode", enum_mode_i32, DAEDALUS_PORTS(value), DAEDALUS_PORTS(out), {
  def.set_label("EnumMode");
  def.set_metadata_json("{\"category\":\"enum\"}");
})
DAEDALUS_NODE_WITH("example_cpp:sync_a_only", sync_a_only_i32, DAEDALUS_PORTS(a, b), DAEDALUS_PORTS(out), {
  def.set_label("SyncAOnly");
  def.set_metadata_json("{\"category\":\"sync\"}");
  def.add_sync_group_ports(DAEDALUS_PORTS(a));
})
DAEDALUS_NODE_WITH("example_cpp:scale_cfg", scale_cfg_i32, DAEDALUS_PORTS(value, cfg), DAEDALUS_PORTS(out), {
  def.set_label("ScaleCfg");
  def.set_metadata_json("{\"category\":\"config\"}");
  const std::string json = daedalus::Codec<ScaleCfg>::encode(ScaleCfg{});
  def.set_input_const_json("cfg", json.c_str());
})
DAEDALUS_STATEFUL_NODE("example_cpp:counter", counter_i32, DAEDALUS_PORTS(inc), DAEDALUS_PORTS(out), "{\"start\":0}")

// Shader-only node (runs on Rust GPU side). Emits a manifest entry with a WGSL file reference.
DAEDALUS_REGISTER_SHADER_NODE_T(
    example_cpp_shader_write_u32,
    "example_cpp:shader_write_u32",
    DAEDALUS_NAMES(),
    std::tuple<>{},
    DAEDALUS_PORTS(out),
    std::tuple<uint32_t>{},
    daedalus::shader().file("shaders/write_u32.wgsl").shader_name("write_u32").invocations(1, 1, 1).storage_u32_rw(0, "out", 4, true))

// Export plugin manifest symbol (single-artifact flow).
DAEDALUS_PLUGIN("example_cpp", "1.0.0", "C/C++ example project")
