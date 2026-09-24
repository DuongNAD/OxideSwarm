// High-Performance 16x16 Tiled Matrix Multiplication (WGSL Compute Shader)
// C = A x B where A is (M x K), B is (K x N), C is (M x N) in row-major layout.

struct Dimensions {
    m: u32,
    k: u32,
    n: u32,
    pad: u32, // Enforces 16-byte uniform alignment mandated by WebGPU
};

@group(0) @binding(0)
var<uniform> dims: Dimensions;

@group(0) @binding(1)
var<storage, read> matrix_a: array<f32>;

@group(0) @binding(2)
var<storage, read> matrix_b: array<f32>;

@group(0) @binding(3)
var<storage, read_write> matrix_c: array<f32>;

// Workgroup shared memory tiles (2 KB total, well within 16 KB minimum limit)
var<workgroup> tile_a: array<array<f32, 16>, 16>;
var<workgroup> tile_b: array<array<f32, 16>, 16>;

@compute @workgroup_size(16, 16, 1)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) group_id: vec3<u32>
) {
    let tx = local_id.x;
    let ty = local_id.y;

    let global_row = group_id.y * 16u + ty;
    let global_col = group_id.x * 16u + tx;

    var acc: f32 = 0.0;
    let num_tiles: u32 = (dims.k + 15u) / 16u;

    for (var t: u32 = 0u; t < num_tiles; t = t + 1u) {
        // 1. Load A tile into shared memory with bounds checking
        let a_col = t * 16u + tx;
        if (global_row < dims.m && a_col < dims.k) {
            tile_a[ty][tx] = matrix_a[global_row * dims.k + a_col];
        } else {
            tile_a[ty][tx] = 0.0;
        }

        // 2. Load B tile into shared memory with bounds checking
        let b_row = t * 16u + ty;
        if (b_row < dims.k && global_col < dims.n) {
            tile_b[ty][tx] = matrix_b[b_row * dims.n + global_col];
        } else {
            tile_b[ty][tx] = 0.0;
        }

        // Synchronize all threads within workgroup to ensure tiles are fully populated
        workgroupBarrier();

        // 3. Compute partial dot-product over 16 elements
        for (var i: u32 = 0u; i < 16u; i = i + 1u) {
            acc = acc + tile_a[ty][i] * tile_b[i][tx];
        }

        // Synchronize before next tile iteration to avoid write-after-read hazards
        workgroupBarrier();
    }

    // 4. Write accumulated result to global Matrix C if within bounds
    if (global_row < dims.m && global_col < dims.n) {
        matrix_c[global_row * dims.n + global_col] = acc;
    }
}
