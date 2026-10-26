//! Zero-dependency 3D vector and transform math utilities.

use std::ops::{Add, AddAssign, Div, Mul, Neg, Sub, SubAssign};

/// Self-contained 3D vector for procedural SDF evaluation and mesh geometry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec3 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

impl Vec3 {
    pub const ZERO: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 0.0,
    };
    pub const ONE: Self = Self {
        x: 1.0,
        y: 1.0,
        z: 1.0,
    };
    pub const UP: Self = Self {
        x: 0.0,
        y: 1.0,
        z: 0.0,
    };

    #[inline(always)]
    pub const fn new(x: f32, y: f32, z: f32) -> Self {
        Self { x, y, z }
    }

    #[inline(always)]
    pub fn dot(self, other: Self) -> f32 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    #[inline(always)]
    pub fn cross(self, other: Self) -> Self {
        Self {
            x: self.y * other.z - self.z * other.y,
            y: self.z * other.x - self.x * other.z,
            z: self.x * other.y - self.y * other.x,
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
    pub fn normalize(self) -> Self {
        let len = self.length();
        if len > 1e-8 {
            self * (1.0 / len)
        } else {
            Self::UP
        }
    }

    #[inline(always)]
    pub fn abs(self) -> Self {
        Self::new(self.x.abs(), self.y.abs(), self.z.abs())
    }

    #[inline(always)]
    pub fn max_elem(self) -> f32 {
        self.x.max(self.y.max(self.z))
    }

    #[inline(always)]
    pub fn min_elem(self) -> f32 {
        self.x.min(self.y.min(self.z))
    }

    #[inline(always)]
    pub fn component_max(self, other: Self) -> Self {
        Self::new(
            self.x.max(other.x),
            self.y.max(other.y),
            self.z.max(other.z),
        )
    }

    #[inline(always)]
    pub fn component_min(self, other: Self) -> Self {
        Self::new(
            self.x.min(other.x),
            self.y.min(other.y),
            self.z.min(other.z),
        )
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

impl AddAssign for Vec3 {
    #[inline(always)]
    fn add_assign(&mut self, rhs: Self) {
        self.x += rhs.x;
        self.y += rhs.y;
        self.z += rhs.z;
    }
}

impl Sub for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn sub(self, rhs: Self) -> Self {
        Self::new(self.x - rhs.x, self.y - rhs.y, self.z - rhs.z)
    }
}

impl SubAssign for Vec3 {
    #[inline(always)]
    fn sub_assign(&mut self, rhs: Self) {
        self.x -= rhs.x;
        self.y -= rhs.y;
        self.z -= rhs.z;
    }
}

impl Mul<f32> for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn mul(self, rhs: f32) -> Self {
        Self::new(self.x * rhs, self.y * rhs, self.z * rhs)
    }
}

impl Mul<Vec3> for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn mul(self, rhs: Vec3) -> Self {
        Self::new(self.x * rhs.x, self.y * rhs.y, self.z * rhs.z)
    }
}

impl Div<f32> for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn div(self, rhs: f32) -> Self {
        Self::new(self.x / rhs, self.y / rhs, self.z / rhs)
    }
}

impl Neg for Vec3 {
    type Output = Self;
    #[inline(always)]
    fn neg(self) -> Self {
        Self::new(-self.x, -self.y, -self.z)
    }
}
