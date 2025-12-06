const MAX_POINTS: u32 = 2048u;
const MAX_STACK: u32 = 4096u;
const PI: f32 = 3.1415926535;

struct QuadParams {
    epsilon_scale: f32,
    min_perimeter: f32,
    min_edge: f32,
    min_angle: f32,
    max_angle: f32,
    contour_count: u32,
    total_points: u32,
    _pad: u32,
};

struct ContourMeta {
    offset: u32,
    length: u32,
};

@group(0) @binding(0)
var<storage, read> contourMeta : array<ContourMeta>;

@group(0) @binding(1)
var<storage, read> contourPoints : array<vec2<f32>>;

@group(0) @binding(2)
var<storage, read_write> quadOutput : array<vec2<f32>>;

@group(0) @binding(3)
var<storage, read_write> quadKeep : array<u32>;

@group(0) @binding(4)
var<uniform> quadParams : QuadParams;

fn distance(a: vec2<f32>, b: vec2<f32>) -> f32 {
    return length(a - b);
}

fn point_line_distance(p: vec2<f32>, a: vec2<f32>, b: vec2<f32>) -> f32 {
    let denom = max(distance(a, b), 1e-5);
    return abs((b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x)) / denom;
}

fn clamp_angle(value: f32) -> f32 {
    return clamp(value, -1.0, 1.0);
}

fn compute_angle(prev: vec2<f32>, curr: vec2<f32>, next: vec2<f32>) -> f32 {
    let v1 = normalize(prev - curr);
    let v2 = normalize(next - curr);
    let dot_val = clamp_angle(dot(v1, v2));
    return degrees(acos(dot_val));
}

fn magnitude_for_index(index: u32) -> f32 {
    if (index == 0u) {
        return 1.0;
    }
    return 0.8;
}

fn ramp_for_index(index: u32) -> f32 {
    if (index == 0u) {
        return 1.0;
    }
    if (index == 1u) {
        return 1.5;
    }
    return 2.1;
}

fn quad_edges_valid(quad: array<vec2<f32>, 4>, min_edge: f32, min_angle: f32, max_angle: f32) -> bool {
    let q0 = quad[0u];
    let q1 = quad[1u];
    let q2 = quad[2u];
    let q3 = quad[3u];

    if (distance(q0, q1) < min_edge) {
        return false;
    }
    if (distance(q1, q2) < min_edge) {
        return false;
    }
    if (distance(q2, q3) < min_edge) {
        return false;
    }
    if (distance(q3, q0) < min_edge) {
        return false;
    }

    let angle0 = compute_angle(q3, q0, q1);
    if (angle0 < min_angle || angle0 > max_angle) {
        return false;
    }
    let angle1 = compute_angle(q0, q1, q2);
    if (angle1 < min_angle || angle1 > max_angle) {
        return false;
    }
    let angle2 = compute_angle(q1, q2, q3);
    if (angle2 < min_angle || angle2 > max_angle) {
        return false;
    }
    let angle3 = compute_angle(q2, q3, q0);
    if (angle3 < min_angle || angle3 > max_angle) {
        return false;
    }

    return true;
}

fn douglas_peucker(offset: u32, length: u32, epsilon: f32, keep: ptr<function, array<u32, MAX_POINTS>>, temp_stack_start: ptr<function, array<i32, MAX_STACK>>, temp_stack_end: ptr<function, array<i32, MAX_STACK>>) -> u32 {
    for (var i = 0u; i < length; i = i + 1u) {
        (*keep)[i] = 0u;
    }
    (*keep)[0u] = 1u;
    (*keep)[length - 1u] = 1u;

    var stack_len: i32 = 0;
    (*temp_stack_start)[0] = 0;
    (*temp_stack_end)[0] = i32(length) - 1;
    stack_len = 1;

    loop {
        if (stack_len == 0) {
            break;
        }
        stack_len = stack_len - 1;
        let start_idx = (*temp_stack_start)[stack_len];
        let end_idx = (*temp_stack_end)[stack_len];
        if (end_idx - start_idx <= 1) {
            continue;
        }
        let start_point = contourPoints[offset + u32(start_idx)];
        let end_point = contourPoints[offset + u32(end_idx)];
        var max_dist = 0.0;
        var max_idx = -1;
        for (var i = start_idx + 1; i < end_idx; i = i + 1) {
            let point = contourPoints[offset + u32(i)];
            let dist = point_line_distance(point, start_point, end_point);
            if (dist > max_dist) {
                max_dist = dist;
                max_idx = i;
            }
        }
        if (max_idx >= 0 && max_dist > epsilon) {
            (*keep)[u32(max_idx)] = 1u;
            (*temp_stack_start)[stack_len] = start_idx;
            (*temp_stack_end)[stack_len] = max_idx;
            stack_len = stack_len + 1;
            (*temp_stack_start)[stack_len] = max_idx;
            (*temp_stack_end)[stack_len] = end_idx;
            stack_len = stack_len + 1;
        }
    }

    var output_len = 0u;
    for (var i = 0u; i < length; i = i + 1u) {
        if ((*keep)[i] != 0u) {
            output_len = output_len + 1u;
        }
    }
    return output_len;
}

@compute @workgroup_size(64)
fn quad_dp_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let contour_idx = id.x;
    if (contour_idx >= quadParams.contour_count) {
        return;
    }

    let contour_info = contourMeta[contour_idx];
    let offset = contour_info.offset;
    let length = contour_info.length;
    if (length < 4u || length > MAX_POINTS) {
        quadKeep[contour_idx] = 0u;
        return;
    }

    var perimeter = 0.0;
    var prev = contourPoints[offset];
    for (var i = 1u; i < length; i = i + 1u) {
        let curr = contourPoints[offset + i];
        perimeter = perimeter + distance(prev, curr);
        prev = curr;
    }
    perimeter = perimeter + distance(prev, contourPoints[offset]);
    if (perimeter < quadParams.min_perimeter) {
        quadKeep[contour_idx] = 0u;
        return;
    }

    let base_eps = clamp(perimeter * 0.02 * quadParams.epsilon_scale, 0.5, 48.0);
    var keep_flags: array<u32, MAX_POINTS>;
    var stack_start: array<i32, MAX_STACK>;
    var stack_end: array<i32, MAX_STACK>;
    var temp_points: array<vec2<f32>, MAX_POINTS>;
    var success = false;

    for (var mi = 0u; mi < 2u; mi = mi + 1u) {
        if (success) {
            break;
        }
        for (var ri = 0u; ri < 3u; ri = ri + 1u) {
            if (success) {
                break;
            }
            let eps = base_eps * magnitude_for_index(mi) * ramp_for_index(ri);
            let simplified_len = douglas_peucker(offset, length, eps, &keep_flags, &stack_start, &stack_end);
            if (simplified_len != 4u) {
                continue;
            }
            var idx = 0u;
            for (var pi = 0u; pi < length; pi = pi + 1u) {
                if (keep_flags[pi] != 0u) {
                    temp_points[idx] = contourPoints[offset + pi];
                    idx = idx + 1u;
                }
            }
            var quad = array<vec2<f32>, 4>(temp_points[0u], temp_points[1u], temp_points[2u], temp_points[3u]);
            if (quad_edges_valid(quad, quadParams.min_edge, quadParams.min_angle, quadParams.max_angle)) {
                for (var qi = 0u; qi < 4u; qi = qi + 1u) {
                    quadOutput[contour_idx * 4u + qi] = quad[qi];
                }
                quadKeep[contour_idx] = 1u;
                success = true;
                break;
            }
        }
    }

    if (!success) {
        quadKeep[contour_idx] = 0u;
    }
}
