const MAX_POINTS: u32 = 2048u;

struct ContourMeta {
    offset: u32,
    length: u32,
};

struct ContourParams {
    min_perimeter: f32,
    max_perimeter: f32,
    score_min: f32,
    score_max: f32,
    min_convexity: f32,
    contour_count: u32,
    _pad0: u32,
    _pad1: u32,
};

@group(0) @binding(0)
var<storage, read> contourMeta : array<ContourMeta>;

@group(0) @binding(1)
var<storage, read> contourPoints : array<vec2<f32>>;

@group(0) @binding(2)
var<storage, read_write> contourKeep : array<u32>;

@group(0) @binding(3)
var<uniform> contourParams : ContourParams;

fn distance(a: vec2<f32>, b: vec2<f32>) -> f32 {
    return length(a - b);
}

fn polygon_perimeter(offset: u32, length: u32) -> f32 {
    if (length == 0u) {
        return 0.0;
    }
    var sum = 0.0;
    var prev = contourPoints[offset + length - 1u];
    for (var i = 0u; i < length; i = i + 1u) {
        let curr = contourPoints[offset + i];
        sum = sum + distance(prev, curr);
        prev = curr;
    }
    return sum;
}

fn polygon_area(offset: u32, length: u32) -> f32 {
    if (length < 3u) {
        return 0.0;
    }
    var area = 0.0;
    var prev = contourPoints[offset + length - 1u];
    for (var i = 0u; i < length; i = i + 1u) {
        let curr = contourPoints[offset + i];
        area = area + (prev.x * curr.y - curr.x * prev.y);
        prev = curr;
    }
    return abs(area) * 0.5;
}

fn cross(o: vec2<f32>, a: vec2<f32>, b: vec2<f32>) -> f32 {
    return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
}

fn sort_points(offset: u32, length: u32, buffer: ptr<function, array<vec2<f32>, MAX_POINTS>>) {
    for (var i = 0u; i < length; i = i + 1u) {
        (*buffer)[i] = contourPoints[offset + i];
    }
    for (var i = 1u; i < length; i = i + 1u) {
        let key = (*buffer)[i];
        var j = i;
        loop {
            if (j == 0u) {
                break;
            }
            let prev = (*buffer)[j - 1u];
            if (prev.x < key.x || (prev.x == key.x && prev.y <= key.y)) {
                break;
            }
            (*buffer)[j] = prev;
            j = j - 1u;
        }
        (*buffer)[j] = key;
    }
}

fn convex_hull_area(offset: u32, length: u32) -> f32 {
    if (length < 3u) {
        return 0.0;
    }
    var sorted: array<vec2<f32>, MAX_POINTS>;
    sort_points(offset, length, &sorted);
    var hull: array<vec2<f32>, MAX_POINTS>;
    var hull_len: u32 = 0u;

    for (var i = 0u; i < length; i = i + 1u) {
        let point = sorted[i];
        loop {
            if (hull_len < 2u) {
                break;
            }
            let cross_val = cross(hull[hull_len - 2u], hull[hull_len - 1u], point);
            if (cross_val > 0.0) {
                break;
            }
            hull_len = hull_len - 1u;
        }
        hull[hull_len] = point;
        hull_len = hull_len + 1u;
    }

    let lower_len = hull_len;
    if (length > 0u) {
        var i = length;
        loop {
            if (i == 0u) {
                break;
            }
            i = i - 1u;
            let point = sorted[i];
            loop {
                if (hull_len <= lower_len) {
                    break;
                }
                if (hull_len < 2u) {
                    break;
                }
                let cross_val = cross(hull[hull_len - 2u], hull[hull_len - 1u], point);
                if (cross_val > 0.0) {
                    break;
                }
                hull_len = hull_len - 1u;
            }
            hull[hull_len] = point;
            hull_len = hull_len + 1u;
        }
    }

    if (hull_len > 1u) {
        hull_len = hull_len - 1u;
    }
    if (hull_len < 3u) {
        return 0.0;
    }

    var area = 0.0;
    for (var i = 0u; i < hull_len; i = i + 1u) {
        let curr = hull[i];
        let next = hull[(i + 1u) % hull_len];
        area = area + (curr.x * next.y - curr.y * next.x);
    }
    return abs(area) * 0.5;
}

@compute @workgroup_size(64)
fn contour_filter_main(@builtin(global_invocation_id) id: vec3<u32>) {
    let contour_idx = id.x;
    if (contour_idx >= contourParams.contour_count) {
        return;
    }
    let contour_info = contourMeta[contour_idx];
    let offset = contour_info.offset;
    let length = contour_info.length;
    if (length < 4u || length > MAX_POINTS) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    let perimeter = polygon_perimeter(offset, length);
    if (perimeter < contourParams.min_perimeter) {
        contourKeep[contour_idx] = 0u;
        return;
    }
    if (contourParams.max_perimeter > 0.0 && perimeter > contourParams.max_perimeter) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    let area = polygon_area(offset, length);
    if (area <= 1e-5) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    let score = (perimeter * perimeter) / area;
    if (score < contourParams.score_min || score > contourParams.score_max) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    let hull_area = convex_hull_area(offset, length);
    if (hull_area <= 1e-5) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    let convexity = area / hull_area;
    if (convexity < contourParams.min_convexity) {
        contourKeep[contour_idx] = 0u;
        return;
    }

    contourKeep[contour_idx] = 1u;
}
