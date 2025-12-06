struct ComponentParams {
    width: u32,
    height: u32,
    min_area: u32,
    _pad: u32,
};

struct ComponentStats {
    min_x: atomic<u32>,
    min_y: atomic<u32>,
    max_x: atomic<u32>,
    max_y: atomic<u32>,
    sum_x_lo: atomic<u32>,
    sum_x_hi: atomic<u32>,
    sum_y_lo: atomic<u32>,
    sum_y_hi: atomic<u32>,
};

@group(0) @binding(0)
var<storage, read> statsLabels : array<u32>;

@group(0) @binding(1)
var<storage, read_write> statsBuffer : array<ComponentStats>;

@group(0) @binding(2)
var<uniform> statsParams : ComponentParams;

struct ComponentSummary {
    area: u32,
    min_x: u32,
    min_y: u32,
    max_x: u32,
    max_y: u32,
    sum_x_lo: u32,
    sum_x_hi: u32,
    sum_y_lo: u32,
    sum_y_hi: u32,
};

@group(0) @binding(3)
var<storage, read_write> summaryBuffer : array<ComponentSummary>;

@group(0) @binding(4)
var<storage, read_write> summaryCounter : array<atomic<u32>>;

@compute @workgroup_size(256)
fn component_stats_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = statsParams.width;
    let height = statsParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }

    let label = statsLabels[idx];
    if (label == 0u) {
        return;
    }

    let x = idx % width;
    let y = idx / width;
    atomicMin(&statsBuffer[label].min_x, x);
    atomicMin(&statsBuffer[label].min_y, y);
    atomicMax(&statsBuffer[label].max_x, x);
    atomicMax(&statsBuffer[label].max_y, y);
    let prev_x = atomicAdd(&statsBuffer[label].sum_x_lo, x);
    if (prev_x + x < prev_x) {
        atomicAdd(&statsBuffer[label].sum_x_hi, 1u);
    }
    let prev_y = atomicAdd(&statsBuffer[label].sum_y_lo, y);
    if (prev_y + y < prev_y) {
        atomicAdd(&statsBuffer[label].sum_y_hi, 1u);
    }
}

@compute @workgroup_size(256)
fn component_reduce_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = statsParams.width * statsParams.height + 1u;
    if (idx >= total) {
        return;
    }
    if (idx == 0u) {
        return;
    }
    let area = statsLabels[idx];
    if (area < statsParams.min_area) {
        return;
    }
    let min_x_val = atomicLoad(&statsBuffer[idx].min_x);
    if (min_x_val == 0xFFFFu) {
        return;
    }
    let slot = atomicAdd(&summaryCounter[0], 1u);
    summaryBuffer[slot].area = area;
    summaryBuffer[slot].min_x = min_x_val;
    summaryBuffer[slot].min_y = atomicLoad(&statsBuffer[idx].min_y);
    summaryBuffer[slot].max_x = atomicLoad(&statsBuffer[idx].max_x);
    summaryBuffer[slot].max_y = atomicLoad(&statsBuffer[idx].max_y);
    summaryBuffer[slot].sum_x_lo = atomicLoad(&statsBuffer[idx].sum_x_lo);
    summaryBuffer[slot].sum_x_hi = atomicLoad(&statsBuffer[idx].sum_x_hi);
    summaryBuffer[slot].sum_y_lo = atomicLoad(&statsBuffer[idx].sum_y_lo);
    summaryBuffer[slot].sum_y_hi = atomicLoad(&statsBuffer[idx].sum_y_hi);
}
