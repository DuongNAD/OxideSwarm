//! Pure-Rust 3D Linear Algebra & Cardano/Smith Eigen-Decomposition for Skeletal Rigging.
//!
//! Provides zero-external-dependency `Vec3`, `Mat4`, `Quat`, rigid matrix inversion,
//! and closed-form 3x3 real symmetric eigen-decomposition.

use std::ops::{Add, Div, Mul, Neg, Sub};

/// 3D Vector for rigging geometry and spatial landmarks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec3 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

impl Vec3 {
    pub const ZERO: Self = Self { x: 0.0, y: 0.0, z: 0.0 };
    pub const ONE: Self = Self { x: 1.0, y: 1.0, z: 1.0 };
    pub const UP: Self = Self { x: 0.0, y: 1.0, z: 0.0 };
    pub const DOWN: Self = Self { x: 0.0, y: -1.0, z: 0.0 };
    pub const FORWARD: Self = Self { x: 0.0, y: 0.0, z: 1.0 };
    pub const BACK: Self = Self { x: 0.0, y: 0.0, z: -1.0 };
    pub const RIGHT: Self = Self { x: 1.0, y: 0.0, z: 0.0 };
    pub const LEFT: Self = Self { x: -1.0, y: 0.0, z: 0.0 };

    #[inline(always)]
    pub const fn new(x: f32, y: f32, z: f32) -> Self {
        Self { x, y, z }
    }

    #[inline(always)]
    pub fn dot(self, rhs: Self) -> f32 {
        self.x * rhs.x + self.y * rhs.y + self.z * rhs.z
    }

    #[inline(always)]
    pub fn cross(self, rhs: Self) -> Self {
        Self {
            x: self.y * rhs.z - self.z * rhs.y,
            y: self.z * rhs.x - self.x * rhs.z,
            z: self.x * rhs.y - self.y * rhs.x,
        }
    }

    #[inline(always)]
    pub fn length_sq(self) -> f32 {
        self.dot(self)
    }

    #[inline(always)]
    pub fn length(self) -> f32 {
        self.length_sq().sqrt()
    }

    #[inline(always)]
    pub fn distance(self, rhs: Self) -> f32 {
        (self - rhs).length()
    }

    #[inline(always)]
    pub fn normalize(self) -> Self {
        let len = self.length();
        if len > 1e-8 {
            Self::new(self.x / len, self.y / len, self.z / len)
        } else {
            Self::UP
        }
    }

    #[inline(always)]
    pub fn to_array(self) -> [f32; 3] {
        [self.x, self.y, self.z]
    }

    #[inline(always)]
    pub fn from_array(arr: [f32; 3]) -> Self {
        Self::new(arr[0], arr[1], arr[2])
    }
}

impl Add for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn add(self, rhs: Self) -> Self {
        Self::new(self.x + rhs.x, self.y + rhs.y, self.z + rhs.z)
    }
}

impl Sub for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn sub(self, rhs: Self) -> Self {
        Self::new(self.x - rhs.x, self.y - rhs.y, self.z - rhs.z)
    }
}

impl Neg for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn neg(self) -> Self {
        Self::new(-self.x, -self.y, -self.z)
    }
}

impl Mul<f32> for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn mul(self, rhs: f32) -> Self {
        Self::new(self.x * rhs, self.y * rhs, self.z * rhs)
    }
}

impl Div<f32> for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn div(self, rhs: f32) -> Self {
        let inv = 1.0 / rhs;
        Self::new(self.x * inv, self.y * inv, self.z * inv)
    }
}

/// Unit Quaternion [x, y, z, w] representing 3D spatial rotations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quat {
    pub x: f32,
    pub y: f32,
    pub z: f32,
    pub w: f32,
}

impl Quat {
    pub const IDENTITY: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 0.0,
        w: 1.0,
    };

    #[inline(always)]
    pub const fn new(x: f32, y: f32, z: f32, w: f32) -> Self {
        Self { x, y, z, w }
    }

    #[inline(always)]
    pub fn to_array(self) -> [f32; 4] {
        [self.x, self.y, self.z, self.w]
    }

    #[inline(always)]
    pub fn from_array(arr: [f32; 4]) -> Self {
        Self::new(arr[0], arr[1], arr[2], arr[3])
    }

    /// Creates a unit quaternion from an axis and rotation angle (in radians).
    #[inline]
    pub fn from_axis_angle(axis: Vec3, angle_rad: f32) -> Self {
        let half = angle_rad * 0.5;
        let s = half.sin();
        let c = half.cos();
        let norm_axis = axis.normalize();
        Self {
            x: norm_axis.x * s,
            y: norm_axis.y * s,
            z: norm_axis.z * s,
            w: c,
        }
    }

    /// Creates a unit quaternion from Tait-Bryan Euler angles:
    /// `pitch` (X), `yaw` (Y), and `roll` (Z) in intrinsic Y-X-Z order.
    #[inline]
    pub fn from_euler(pitch: f32, yaw: f32, roll: f32) -> Self {
        let cx = (pitch * 0.5).cos();
        let sx = (pitch * 0.5).sin();
        let cy = (yaw * 0.5).cos();
        let sy = (yaw * 0.5).sin();
        let cz = (roll * 0.5).cos();
        let sz = (roll * 0.5).sin();

        Self {
            x: cy * sx * cz + sy * cx * sz,
            y: sy * cx * cz - cy * sx * sz,
            z: cy * cx * sz - sy * sx * cz,
            w: cy * cx * cz + sy * sx * sz,
        }
        .normalize()
    }

    #[inline(always)]
    pub fn dot(self, rhs: Self) -> f32 {
        self.x * rhs.x + self.y * rhs.y + self.z * rhs.z + self.w * rhs.w
    }

    #[inline(always)]
    pub fn length(self) -> f32 {
        (self.dot(self)).sqrt()
    }

    #[inline(always)]
    pub fn normalize(self) -> Self {
        let len = self.length();
        if len > 1e-8 {
            let inv = 1.0 / len;
            Self::new(self.x * inv, self.y * inv, self.z * inv, self.w * inv)
        } else {
            Self::IDENTITY
        }
    }

    /// Hamilton quaternion product: `self * rhs`.
    #[inline]
    pub fn mul_quat(&self, rhs: &Self) -> Self {
        Self {
            x: self.w * rhs.x + self.x * rhs.w + self.y * rhs.z - self.z * rhs.y,
            y: self.w * rhs.y - self.x * rhs.z + self.y * rhs.w + self.z * rhs.x,
            z: self.w * rhs.z + self.x * rhs.y - self.y * rhs.x + self.z * rhs.w,
            w: self.w * rhs.w - self.x * rhs.x - self.y * rhs.y - self.z * rhs.z,
        }
    }

    /// Rotates a 3D vector by this quaternion.
    #[inline]
    pub fn rotate_vec3(&self, v: Vec3) -> Vec3 {
        let q_vec = Vec3::new(self.x, self.y, self.z);
        let uv = q_vec.cross(v);
        let uuv = q_vec.cross(uv);
        v + (uv * self.w + uuv) * 2.0
    }

    /// Spherical Linear Interpolation (Slerp) with shortest-path inversion
    /// and normalized lerp fallback for near-collinear quaternions.
    #[inline]
    pub fn slerp(q1: Self, mut q2: Self, t: f32) -> Self {
        let mut dot = q1.dot(q2);
        if dot < 0.0 {
            q2 = Self::new(-q2.x, -q2.y, -q2.z, -q2.w);
            dot = -dot;
        }

        if dot > 0.9995 {
            // NLERP fallback
            let inv_t = 1.0 - t;
            Self::new(
                inv_t * q1.x + t * q2.x,
                inv_t * q1.y + t * q2.y,
                inv_t * q1.z + t * q2.z,
                inv_t * q1.w + t * q2.w,
            )
            .normalize()
        } else {
            let theta_0 = dot.clamp(-1.0, 1.0).acos();
            let theta = theta_0 * t;
            let sin_theta = theta.sin();
            let sin_theta_0 = theta_0.sin();

            let s0 = (theta_0 - theta).sin() / sin_theta_0;
            let s1 = sin_theta / sin_theta_0;

            Self::new(
                s0 * q1.x + s1 * q2.x,
                s0 * q1.y + s1 * q2.y,
                s0 * q1.z + s1 * q2.z,
                s0 * q1.w + s1 * q2.w,
            )
            .normalize()
        }
    }
}

/// 4x4 Column-Major Affine Transformation Matrix for skeletal animation and Inverse Bind Matrices.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Mat4 {
    /// Column-major storage: `cols[col][row]`
    pub cols: [[f32; 4]; 4],
}

impl Mat4 {
    pub const IDENTITY: Self = Self {
        cols: [
            [1.0, 0.0, 0.0, 0.0], // Col 0
            [0.0, 1.0, 0.0, 0.0], // Col 1
            [0.0, 0.0, 1.0, 0.0], // Col 2
            [0.0, 0.0, 0.0, 1.0], // Col 3
        ],
    };

    #[inline]
    pub const fn from_cols(cols: [[f32; 4]; 4]) -> Self {
        Self { cols }
    }

    /// Constructs a translation matrix.
    #[inline]
    pub fn from_translation(t: Vec3) -> Self {
        Self {
            cols: [
                [1.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0],
                [t.x, t.y, t.z, 1.0],
            ],
        }
    }

    /// Constructs a transformation matrix from Translation, Rotation (Quat), and Scale.
    #[inline]
    pub fn from_trs(t: Vec3, r: Quat, s: Vec3) -> Self {
        let x2 = r.x + r.x;
        let y2 = r.y + r.y;
        let z2 = r.z + r.z;

        let xx = r.x * x2;
        let xy = r.x * y2;
        let xz = r.x * z2;
        let yy = r.y * y2;
        let yz = r.y * z2;
        let zz = r.z * z2;
        let wx = r.w * x2;
        let wy = r.w * y2;
        let wz = r.w * z2;

        Self {
            cols: [
                [(1.0 - (yy + zz)) * s.x, (xy + wz) * s.x, (xz - wy) * s.x, 0.0],
                [(xy - wz) * s.y, (1.0 - (xx + zz)) * s.y, (yz + wx) * s.y, 0.0],
                [(xz + wy) * s.z, (yz - wx) * s.z, (1.0 - (xx + yy)) * s.z, 0.0],
                [t.x, t.y, t.z, 1.0],
            ],
        }
    }

    /// Matrix multiplication: `self * rhs`.
    #[inline]
    pub fn mul_mat4(&self, rhs: &Self) -> Self {
        let mut out = [[0.0f32; 4]; 4];
        for col in 0..4 {
            for row in 0..4 {
                out[col][row] = self.cols[0][row] * rhs.cols[col][0]
                    + self.cols[1][row] * rhs.cols[col][1]
                    + self.cols[2][row] * rhs.cols[col][2]
                    + self.cols[3][row] * rhs.cols[col][3];
            }
        }
        Self { cols: out }
    }

    /// Exact inverse for rigid affine transformations (Rotation + Translation).
    ///
    /// For W = [ R  T ], W^(-1) = [ R^T  -R^T * T ]
    ///         [ 0  1 ]           [ 0       1     ]
    #[inline]
    pub fn inverse_rigid(&self) -> Self {
        // Transpose 3x3 upper-left rotation block
        let r00 = self.cols[0][0]; let r01 = self.cols[1][0]; let r02 = self.cols[2][0];
        let r10 = self.cols[0][1]; let r11 = self.cols[1][1]; let r12 = self.cols[2][1];
        let r20 = self.cols[0][2]; let r21 = self.cols[1][2]; let r22 = self.cols[2][2];

        // Translation vector
        let tx = self.cols[3][0];
        let ty = self.cols[3][1];
        let tz = self.cols[3][2];

        // Inverse translation: -R^T * T
        let itx = -(r00 * tx + r10 * ty + r20 * tz);
        let ity = -(r01 * tx + r11 * ty + r21 * tz);
        let itz = -(r02 * tx + r12 * ty + r22 * tz);

        Self {
            cols: [
                [r00, r01, r02, 0.0],
                [r10, r11, r12, 0.0],
                [r20, r21, r22, 0.0],
                [itx, ity, itz, 1.0],
            ],
        }
    }

    /// Transforms a 3D point by this 4x4 matrix (assuming w = 1.0).
    #[inline]
    pub fn transform_point3(&self, p: Vec3) -> Vec3 {
        Vec3::new(
            self.cols[0][0] * p.x + self.cols[1][0] * p.y + self.cols[2][0] * p.z + self.cols[3][0],
            self.cols[0][1] * p.x + self.cols[1][1] * p.y + self.cols[2][1] * p.z + self.cols[3][1],
            self.cols[0][2] * p.x + self.cols[1][2] * p.y + self.cols[2][2] * p.z + self.cols[3][2],
        )
    }

    /// Serializes matrix to a 16-element column-major float array compliant with glTF 2.0.
    #[inline]
    pub fn to_column_major_array(&self) -> [f32; 16] {
        [
            self.cols[0][0], self.cols[0][1], self.cols[0][2], self.cols[0][3],
            self.cols[1][0], self.cols[1][1], self.cols[1][2], self.cols[1][3],
            self.cols[2][0], self.cols[2][1], self.cols[2][2], self.cols[2][3],
            self.cols[3][0], self.cols[3][1], self.cols[3][2], self.cols[3][3],
        ]
    }
}

/// 3x3 Real Symmetric Eigen-Decomposition output.
#[derive(Debug, Clone, PartialEq)]
pub struct EigenDecomposition3 {
    /// Eigenvalues in descending order: lambda1 >= lambda2 >= lambda3 >= 0.
    pub eigenvalues: [f32; 3],
    /// Corresponding orthonormal eigenvectors (unit length).
    pub eigenvectors: [Vec3; 3],
}

/// Computes the closed-form real symmetric 3x3 eigen-decomposition of matrix `C`
/// using Smith's trigonometric Cardano solver.
#[inline]
pub fn eigen_decomposition_3x3(c: [[f32; 3]; 3]) -> EigenDecomposition3 {
    eigen_symmetric_3x3(c)
}

/// Computes the closed-form real symmetric 3x3 eigen-decomposition of matrix `C`
/// using Smith's trigonometric Cardano solver.
///
/// Input matrix `c` is given as symmetric 3x3:
/// `[[c00, c01, c02], [c01, c11, c12], [c02, c12, c22]]`
pub fn eigen_symmetric_3x3(c: [[f32; 3]; 3]) -> EigenDecomposition3 {
    let c00 = c[0][0]; let c01 = c[0][1]; let c02 = c[0][2];
    let c11 = c[1][1]; let c12 = c[1][2];
    let c22 = c[2][2];

    // 1. Mean trace
    let m = (c00 + c11 + c22) / 3.0;

    // 2. Shifted matrix B = C - m * I
    let b00 = c00 - m; let b11 = c11 - m; let b22 = c22 - m;
    let b01 = c01; let b02 = c02; let b12 = c12;

    // 3. Invariants
    let q = 0.5 * (b00 * (b11 * b22 - b12 * b12) - b01 * (b01 * b22 - b02 * b12) + b02 * (b01 * b12 - b02 * b11));
    let p = (b00 * b00 + b11 * b11 + b22 * b22 + 2.0 * (b01 * b01 + b02 * b02 + b12 * b12)) / 6.0;

    if p < 1e-12 {
        // Isotropic case: all eigenvalues equal m
        return EigenDecomposition3 {
            eigenvalues: [m, m, m],
            eigenvectors: [Vec3::RIGHT, Vec3::UP, Vec3::FORWARD],
        };
    }

    let sqrt_p = p.sqrt();
    let phi_arg = (q / (p * sqrt_p)).clamp(-1.0, 1.0);
    let phi = phi_arg.acos() / 3.0;

    // 4. Eigenvalues
    let two_pi_over_3 = std::f32::consts::FRAC_PI_3 * 2.0;
    let lambda1 = m + 2.0 * sqrt_p * phi.cos();
    let lambda2 = m + 2.0 * sqrt_p * (phi + two_pi_over_3).cos();
    let lambda3 = m + 2.0 * sqrt_p * (phi + 2.0 * two_pi_over_3).cos();

    // Sort descending
    let mut evals = [lambda1, lambda2, lambda3];
    evals.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

    // 5. Compute eigenvectors for each eigenvalue
    let e1 = compute_eigenvector(c, evals[0], Vec3::RIGHT);
    let mut e2 = compute_eigenvector(c, evals[1], Vec3::UP);

    // Ensure e2 is orthogonal to e1 (Gram-Schmidt)
    e2 = (e2 - e1 * e2.dot(e1)).normalize();

    // e3 is cross product to guarantee right-handed orthonormal basis
    let e3 = e1.cross(e2).normalize();

    EigenDecomposition3 {
        eigenvalues: evals,
        eigenvectors: [e1, e2, e3],
    }
}

/// Solves (C - lambda * I) * v = 0 for eigenvector v.
fn compute_eigenvector(c: [[f32; 3]; 3], lambda: f32, fallback: Vec3) -> Vec3 {
    // Rows of (C - lambda * I)
    let r0 = Vec3::new(c[0][0] - lambda, c[0][1], c[0][2]);
    let r1 = Vec3::new(c[1][0], c[1][1] - lambda, c[1][2]);
    let r2 = Vec3::new(c[2][0], c[2][1], c[2][2] - lambda);

    // Cross products of pairs of rows give vectors perpendicular to both rows
    let c01 = r0.cross(r1);
    let c02 = r0.cross(r2);
    let c12 = r1.cross(r2);

    let l01 = c01.length_sq();
    let l02 = c02.length_sq();
    let l12 = c12.length_sq();

    let max_len = l01.max(l02).max(l12);
    if max_len > 1e-10 {
        if (l01 - max_len).abs() < 1e-10 {
            c01.normalize()
        } else if (l02 - max_len).abs() < 1e-10 {
            c02.normalize()
        } else {
            c12.normalize()
        }
    } else {
        fallback.normalize()
    }
}
