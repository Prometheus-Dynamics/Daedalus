struct ComponentParams {
    width: u32,
    height: u32,
    min_area: u32,
    _pad: u32,
};

@group(0) @binding(0)
var<storage, read> initMask : array<u32>;

@group(0) @binding(1)
var<storage, read_write> initLabels : array<u32>;

@group(0) @binding(2)
var<uniform> initParams : ComponentParams;

@group(0) @binding(3)
var<storage, read> propagateSrc : array<u32>;

@group(0) @binding(4)
var<storage, read_write> propagateDst : array<u32>;

@group(0) @binding(5)
var<storage, read_write> propagateChange : atomic<u32>;

@group(0) @binding(6)
var<uniform> propagateParams : ComponentParams;

@group(0) @binding(7)
var<storage, read> countLabels : array<u32>;

@group(0) @binding(8)
var<storage, read_write> countTotals : array<atomic<u32>>;

@group(0) @binding(9)
var<uniform> countParams : ComponentParams;

@group(0) @binding(10)
var<storage, read> filterLabels : array<u32>;

@group(0) @binding(11)
var<storage, read> filterTotals : array<atomic<u32>>;

@group(0) @binding(12)
var<storage, read_write> filterOutput : array<u32>;

@group(0) @binding(13)
var<uniform> filterParams : ComponentParams;

fn pack_gray(value: f32) -> u32 {
    let clamped = clamp(value, 0.0, 1.0);
    let v = u32(clamped * 255.0 + 0.5);
    return v | (v << 8u) | (v << 16u) | (255u << 24u);
}

@compute @workgroup_size(256)
fn component_init_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = initParams.width * initParams.height;
    if (idx >= total) {
        return;
    }

    let pixel = initMask[idx] & 0xFFu;
    if (pixel > 0u) {
        initLabels[idx] = idx + 1u;
    } else {
        initLabels[idx] = 0u;
    }
}

fn min_label(current: u32, candidate: u32) -> u32 {
    if (candidate == 0u) {
        return current;
    }
    return select(current, candidate, candidate < current);
}

@compute @workgroup_size(256)
fn component_propagate_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let width = propagateParams.width;
    let height = propagateParams.height;
    let total = width * height;
    if (idx >= total) {
        return;
    }

    let label = propagateSrc[idx];
    if (label == 0u) {
        propagateDst[idx] = 0u;
        return;
    }

    let x = idx % width;
    let y = idx / width;
    var best = label;

    if (x > 0u) {
        best = min_label(best, propagateSrc[idx - 1u]);
    }
    if (x + 1u < width) {
        best = min_label(best, propagateSrc[idx + 1u]);
    }
    if (y > 0u) {
        best = min_label(best, propagateSrc[idx - width]);
    }
    if (y + 1u < height) {
        best = min_label(best, propagateSrc[idx + width]);
    }

    propagateDst[idx] = best;
    if (best != label) {
        atomicStore(&propagateChange, 1u);
    }
}

@compute @workgroup_size(256)
fn component_count_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = countParams.width * countParams.height;
    if (idx >= total) {
        return;
    }

    let label = countLabels[idx];
    if (label == 0u) {
        return;
    }
    atomicAdd(&countTotals[label], 1u);
}

@compute @workgroup_size(256)
fn component_filter_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let idx = id.x;
    let total = filterParams.width * filterParams.height;
    if (idx >= total) {
        return;
    }

    let label = filterLabels[idx];
    if (label == 0u) {
        filterOutput[idx] = 0u;
        return;
    }

    let area = atomicLoad(&filterTotals[label]);
    if (area >= filterParams.min_area) {
        filterOutput[idx] = pack_gray(1.0);
    } else {
        filterOutput[idx] = 0u;
    }
}
