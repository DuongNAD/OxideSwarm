// Parallel Reduction & Verification Hashing (WGSL Compute Shader)
// Computes sum, sum of squares, trace, and 64-bit FNV-1a verification digest.

struct HashUniforms {
    total_elements: u32,
    dim_m: u32,
    dim_n: u32,
    pad: u32,
};

struct WorkgroupReductionOutput {
    sum: f32,
    f_norm_sq: f32,
    trace: f32,
    hash_lo: u32,
    hash_hi: u32,
    pad0: u32,
    pad1: u32,
    pad2: u32,
};

@group(0) @binding(0)
var<uniform> params: HashUniforms;

@group(0) @binding(1)
var<storage, read> data: array<f32>;

@group(0) @binding(2)
var<storage, read_write> partial_outputs: array<WorkgroupReductionOutput>;

// Workgroup shared memory for tree reduction
var<workgroup> s_sum: array<f32, 256>;
var<workgroup> s_sq: array<f32, 256>;
var<workgroup> s_trace: array<f32, 256>;

// 64-bit unsigned integer multiplication: (a * b) mod 2^64
// Represented as vec2<u32>(low, high)
fn mul64(a: vec2<u32>, b: vec2<u32>) -> vec2<u32> {
    let a_lo = a.x;
    let a_hi = a.y;
    let b_lo = b.x;
    let b_hi = b.y;

    // Split a_lo and b_lo into 16-bit halves to prevent 32-bit overflow
    let a0 = a_lo & 0xFFFFu;
    let a1 = a_lo >> 16u;
    let b0 = b_lo & 0xFFFFu;
    let b1 = b_lo >> 16u;

    let p00 = a0 * b0;
    let p01 = a0 * b1;
    let p10 = a1 * b0;
    let p11 = a1 * b1;

    let mid = p01 + (p00 >> 16u) + (p10 & 0xFFFFu);
    let lo = (p00 & 0xFFFFu) | ((mid & 0xFFFFu) << 16u);
    let carry_out = (mid >> 16u) + p11 + (p10 >> 16u);

    // Cross-term high product
    let hi = carry_out + a_lo * b_hi + a_hi * b_lo;

    return vec2<u32>(lo, hi);
}

// 64-bit FNV-1a step: hash = (hash ^ val_bits) * FNV_PRIME
fn fnv1a_step(current_hash: vec2<u32>, val_bits: u32) -> vec2<u32> {
    let fnv_prime = vec2<u32>(0x000001b3u, 0x00000100u); // 0x100000001b3
    // XOR with 32-bit IEEE float bits
    let xored = vec2<u32>(current_hash.x ^ val_bits, current_hash.y);
    return mul64(xored, fnv_prime);
}

@compute @workgroup_size(256, 1, 1)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) group_id: vec3<u32>,
    @builtin(num_workgroups) num_groups: vec3<u32>
) {
    let tid = local_id.x;
    let grid_stride = num_groups.x * 256u;
    var idx = group_id.x * 256u + tid;

    var local_sum: f32 = 0.0;
    var local_sq: f32 = 0.0;
    var local_trace: f32 = 0.0;
    var local_hash = vec2<u32>(0x84222325u, 0xcbf29ce4u); // FNV-1a 64-bit offset basis

    // Grid-stride accumulation loop
    while (idx < params.total_elements) {
        let val = data[idx];
        local_sum = local_sum + val;
        local_sq = local_sq + val * val;

        // Diagonal elements for square / trace computation: row == col
        let row = idx / params.dim_n;
        let col = idx % params.dim_n;
        if (row == col && row < params.dim_m) {
            local_trace = local_trace + val;
        }

        local_hash = fnv1a_step(local_hash, bitcast<u32>(val));
        idx = idx + grid_stride;
    }

    // Populate shared memory
    s_sum[tid] = local_sum;
    s_sq[tid] = local_sq;
    s_trace[tid] = local_trace;

    // Parallel tree reduction within workgroup
    for (var s: u32 = 128u; s > 0u; s = s >> 1u) {
        workgroupBarrier();
        if (tid < s) {
            s_sum[tid] = s_sum[tid] + s_sum[tid + s];
            s_sq[tid] = s_sq[tid] + s_sq[tid + s];
            s_trace[tid] = s_trace[tid] + s_trace[tid + s];
        }
    }

    // Thread 0 in each workgroup writes partial reduction results
    if (tid == 0u) {
        partial_outputs[group_id.x].sum = s_sum[0];
        partial_outputs[group_id.x].f_norm_sq = s_sq[0];
        partial_outputs[group_id.x].trace = s_trace[0];
        partial_outputs[group_id.x].hash_lo = local_hash.x;
        partial_outputs[group_id.x].hash_hi = local_hash.y;
    }
}
