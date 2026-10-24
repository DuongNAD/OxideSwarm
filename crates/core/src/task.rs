//! Task definitions, specifications, requirements, results, and state machine transitions.

pub use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use uuid::Uuid;

/// Zero-copy, high-performance byte buffer wrapping `bytes::Bytes` with transparent UTF-8 inspection.
#[derive(Clone, Default, Eq)]
#[repr(transparent)]
pub struct Bytes(pub bytes::Bytes);

impl Bytes {
    #[inline]
    pub const fn new() -> Self {
        Bytes(bytes::Bytes::new())
    }

    #[inline]
    pub fn copy_from_slice(data: &[u8]) -> Self {
        Bytes(bytes::Bytes::copy_from_slice(data))
    }

    #[inline]
    pub fn from_static(bytes: &'static [u8]) -> Self {
        Bytes(bytes::Bytes::from_static(bytes))
    }

    /// Returns the longest valid UTF-8 prefix as a string slice.
    ///
    /// This is the view behind `Deref<Target = str>`, so it can only borrow: when the buffer
    /// is not entirely valid UTF-8 (OEM code-page console output, a byte cap that cut a
    /// multibyte character) everything from the first invalid byte on is not visible here.
    /// It used to return `""` for the whole buffer, which silently erased the output. Use
    /// [`Bytes::to_string_lossy`] when the full content matters (forwarding, display, logging)
    /// and [`Bytes::try_as_str`] when invalid data must be detected.
    #[inline]
    pub fn as_str(&self) -> &str {
        match std::str::from_utf8(&self.0) {
            Ok(s) => s,
            Err(e) => std::str::from_utf8(&self.0[..e.valid_up_to()]).unwrap_or(""),
        }
    }

    /// Returns the whole buffer as `&str`, or `None` if it is not valid UTF-8.
    #[inline]
    pub fn try_as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// Returns the whole buffer as text, replacing invalid UTF-8 sequences with U+FFFD.
    /// Borrows (no allocation) when the buffer is valid UTF-8.
    #[inline]
    pub fn to_string_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.0)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
        Bytes(self.0.slice(range))
    }

    #[inline]
    pub fn into_bytes(self) -> bytes::Bytes {
        self.0
    }

    #[inline]
    pub fn inner(&self) -> &bytes::Bytes {
        &self.0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl std::ops::Deref for Bytes {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Bytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<str> for Bytes {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<[u8]> for Bytes {
    #[inline]
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "{:?}", s)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

impl From<bytes::Bytes> for Bytes {
    #[inline]
    fn from(b: bytes::Bytes) -> Self {
        Bytes(b)
    }
}

impl From<Bytes> for bytes::Bytes {
    #[inline]
    fn from(b: Bytes) -> Self {
        b.0
    }
}

impl From<Vec<u8>> for Bytes {
    #[inline]
    fn from(v: Vec<u8>) -> Self {
        Bytes(bytes::Bytes::from(v))
    }
}

impl From<String> for Bytes {
    #[inline]
    fn from(s: String) -> Self {
        Bytes(bytes::Bytes::from(s))
    }
}

impl From<&str> for Bytes {
    #[inline]
    fn from(s: &str) -> Self {
        Bytes(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<&[u8]> for Bytes {
    #[inline]
    fn from(s: &[u8]) -> Self {
        Bytes(bytes::Bytes::copy_from_slice(s))
    }
}

impl PartialEq for Bytes {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

// String comparisons are byte-exact: a buffer that is not valid UTF-8 is never equal to any
// string (it used to compare equal to `""`).
impl PartialEq<str> for Bytes {
    #[inline]
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other.as_bytes()
    }
}

impl PartialEq<&str> for Bytes {
    #[inline]
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == other.as_bytes()
    }
}

impl PartialEq<String> for Bytes {
    #[inline]
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other.as_bytes()
    }
}

impl PartialEq<Bytes> for str {
    #[inline]
    fn eq(&self, other: &Bytes) -> bool {
        self.as_bytes() == other.0.as_ref()
    }
}

impl PartialEq<Bytes> for &str {
    #[inline]
    fn eq(&self, other: &Bytes) -> bool {
        self.as_bytes() == other.0.as_ref()
    }
}

impl PartialEq<Bytes> for String {
    #[inline]
    fn eq(&self, other: &Bytes) -> bool {
        self.as_bytes() == other.0.as_ref()
    }
}

impl std::ops::Index<usize> for Bytes {
    type Output = u8;
    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl FromIterator<u8> for Bytes {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        let vec: Vec<u8> = iter.into_iter().collect();
        Bytes(bytes::Bytes::from(vec))
    }
}

impl<'a> FromIterator<&'a u8> for Bytes {
    fn from_iter<T: IntoIterator<Item = &'a u8>>(iter: T) -> Self {
        let vec: Vec<u8> = iter.into_iter().copied().collect();
        Bytes(bytes::Bytes::from(vec))
    }
}

impl FromIterator<i32> for Bytes {
    fn from_iter<T: IntoIterator<Item = i32>>(iter: T) -> Self {
        let vec: Vec<u8> = iter.into_iter().map(|x| x as u8).collect();
        Bytes(bytes::Bytes::from(vec))
    }
}

impl std::hash::Hash for Bytes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

pub mod bytes_serde {
    use super::Bytes;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            if let Ok(s) = std::str::from_utf8(bytes.as_bytes()) {
                serializer.serialize_str(s)
            } else {
                serializer.serialize_bytes(bytes.as_bytes())
            }
        } else {
            serializer.serialize_bytes(bytes.as_bytes())
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;
        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a byte array, slice, or string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::copy_from_slice(v.as_bytes()))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::copy_from_slice(v))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes::from(v))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut v = Vec::new();
                while let Some(b) = seq.next_element()? {
                    v.push(b);
                }
                Ok(Bytes::from(v))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(BytesVisitor)
        } else {
            deserializer.deserialize_byte_buf(BytesVisitor)
        }
    }
}

impl serde::Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        bytes_serde::serialize(self, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        bytes_serde::deserialize(deserializer)
    }
}

pub mod opt_bytes_serde {
    use super::Bytes;
    use serde::{Deserializer, Serializer};

    struct BytesRefWrapper<'a>(&'a Bytes);
    impl<'a> serde::Serialize for BytesRefWrapper<'a> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            super::bytes_serde::serialize(self.0, serializer)
        }
    }

    pub fn serialize<S>(opt: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt {
            Some(b) => serializer.serialize_some(&BytesRefWrapper(b)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptBytesVisitor;
        impl<'de> serde::de::Visitor<'de> for OptBytesVisitor {
            type Value = Option<Bytes>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an optional byte sequence, string, or array")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                super::bytes_serde::deserialize(deserializer).map(Some)
            }
        }

        deserializer.deserialize_option(OptBytesVisitor)
    }
}

/// Strongly-typed unique identifier for a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub Uuid);

impl TaskId {
    /// Generates a new random (v4) TaskId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a TaskId from an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns a reference to the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consumes self and returns the inner UUID.
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(TaskId)
    }
}

/// Resource requirements and scheduling constraints for a task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRequirements {
    /// Minimum CPU cores required.
    pub cpu_cores: usize,
    /// Minimum system RAM required in MB.
    pub ram_mb: u64,
    /// Whether this task requires a GPU-capable worker.
    pub gpu_required: bool,
    /// Maximum allowable execution time in seconds before cancellation.
    pub timeout_secs: u64,
    /// Maximum allowable retries on worker failure. None inherits cluster default.
    #[serde(default)]
    pub max_retries: Option<u32>,
}

impl Default for TaskRequirements {
    fn default() -> Self {
        Self {
            cpu_cores: 1,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs: 60,
            max_retries: None,
        }
    }
}

impl TaskRequirements {
    pub fn new(cpu_cores: usize, ram_mb: u64, gpu_required: bool, timeout_secs: u64) -> Self {
        Self {
            cpu_cores,
            ram_mb,
            gpu_required,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Helper for generic CPU tasks.
    pub fn generic(cpu_cores: usize, timeout_secs: u64) -> Self {
        Self {
            cpu_cores,
            ram_mb: 512,
            gpu_required: false,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Helper for GPU tasks.
    pub fn gpu(timeout_secs: u64) -> Self {
        Self {
            cpu_cores: 1,
            ram_mb: 1024,
            gpu_required: true,
            timeout_secs,
            max_retries: None,
        }
    }

    /// Configures maximum retry attempts on worker failure or crash.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Validates the requirements specification.
    pub fn validate(&self) -> Result<(), String> {
        if self.timeout_secs == 0 {
            return Err("Task timeout_secs must be greater than 0".to_string());
        }
        Ok(())
    }
}

fn default_work_group_size() -> u32 {
    16
}

fn default_matrix_dim() -> u32 {
    64
}

fn default_test_name() -> String {
    "hash_compute".to_string()
}

fn default_iterations() -> u32 {
    100_000
}

// ============================================================================
// 3D Generation & Auto-Rigging Defaults and Specifications
// ============================================================================

fn default_3d_quality() -> String {
    "standard".to_string()
}

fn default_rig_preset() -> String {
    "biped".to_string()
}

fn default_embed_animations() -> Vec<String> {
    vec![
        "idle".to_string(),
        "walk".to_string(),
        "turnaround".to_string(),
    ]
}

fn default_export_texture() -> bool {
    true
}

/// Specification for local on-device 3D mesh generation from text prompt or input image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generative3DSpec {
    /// Optional natural language text prompt describing the 3D asset (e.g. "a low-poly treasure chest").
    pub prompt: Option<String>,
    /// Optional input image bytes (PNG, JPEG, WebP) for image-to-3D mesh reconstruction.
    pub image_data: Option<Bytes>,
    /// Generation fidelity / quality preset ("fast", "standard", "high"). Defaults to "standard".
    pub quality: String,
    /// Preferred generation engine ("sdf", "triposr", "auto"). Defaults to None ("auto").
    pub engine_preference: Option<String>,
    /// Whether to generate and bake PBR UV texture maps into the output glTF asset. Defaults to true.
    pub export_texture: bool,
}

#[derive(Serialize, Deserialize)]
struct HumanGenerative3DSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prompt: Option<String>,
    #[serde(default, with = "opt_bytes_serde", skip_serializing_if = "Option::is_none")]
    image_data: Option<Bytes>,
    #[serde(default = "default_3d_quality")]
    quality: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    engine_preference: Option<String>,
    #[serde(default = "default_export_texture")]
    pub export_texture: bool,
}

#[derive(Serialize, Deserialize)]
struct BinaryGenerative3DSpec {
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default, with = "opt_bytes_serde")]
    image_data: Option<Bytes>,
    #[serde(default = "default_3d_quality")]
    quality: String,
    #[serde(default)]
    engine_preference: Option<String>,
    #[serde(default = "default_export_texture")]
    pub export_texture: bool,
}

impl From<&Generative3DSpec> for HumanGenerative3DSpec {
    fn from(s: &Generative3DSpec) -> Self {
        Self {
            prompt: s.prompt.clone(),
            image_data: s.image_data.clone(),
            quality: s.quality.clone(),
            engine_preference: s.engine_preference.clone(),
            export_texture: s.export_texture,
        }
    }
}

impl From<HumanGenerative3DSpec> for Generative3DSpec {
    fn from(s: HumanGenerative3DSpec) -> Self {
        Self {
            prompt: s.prompt,
            image_data: s.image_data,
            quality: s.quality,
            engine_preference: s.engine_preference,
            export_texture: s.export_texture,
        }
    }
}

impl From<&Generative3DSpec> for BinaryGenerative3DSpec {
    fn from(s: &Generative3DSpec) -> Self {
        Self {
            prompt: s.prompt.clone(),
            image_data: s.image_data.clone(),
            quality: s.quality.clone(),
            engine_preference: s.engine_preference.clone(),
            export_texture: s.export_texture,
        }
    }
}

impl From<BinaryGenerative3DSpec> for Generative3DSpec {
    fn from(s: BinaryGenerative3DSpec) -> Self {
        Self {
            prompt: s.prompt,
            image_data: s.image_data,
            quality: s.quality,
            engine_preference: s.engine_preference,
            export_texture: s.export_texture,
        }
    }
}

impl Serialize for Generative3DSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanGenerative3DSpec::from(self).serialize(serializer)
        } else {
            BinaryGenerative3DSpec::from(self).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Generative3DSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanGenerative3DSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryGenerative3DSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

impl Generative3DSpec {
    /// Creates a 3D generation spec from a text prompt with standard quality and texture export enabled.
    pub fn new_prompt(prompt: impl Into<String>) -> Self {
        Self {
            prompt: Some(prompt.into()),
            image_data: None,
            quality: default_3d_quality(),
            engine_preference: None,
            export_texture: true,
        }
    }

    /// Creates a 3D generation spec from an input image buffer.
    pub fn new_image(image_data: impl Into<Bytes>) -> Self {
        Self {
            prompt: None,
            image_data: Some(image_data.into()),
            quality: default_3d_quality(),
            engine_preference: None,
            export_texture: true,
        }
    }

    /// Sets the quality preset ("fast", "standard", "high").
    pub fn with_quality(mut self, quality: impl Into<String>) -> Self {
        self.quality = quality.into();
        self
    }

    /// Sets the engine preference ("sdf", "triposr", "auto").
    pub fn with_engine(mut self, engine: impl Into<String>) -> Self {
        self.engine_preference = Some(engine.into());
        self
    }

    /// Sets whether UV textures should be baked and exported.
    pub fn with_export_texture(mut self, export_texture: bool) -> Self {
        self.export_texture = export_texture;
        self
    }
}

/// Specification for local automated skeleton rigging and animation synthesis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoRigSpec {
    /// Input 3D mesh file bytes (glTF/GLB or OBJ binary format).
    pub input_mesh: Bytes,
    /// Target skeletal armature preset ("biped", "quadruped", "generic"). Defaults to "biped".
    pub rig_preset: String,
    /// Standard animation clips to synthesize and embed in the output glTF asset.
    pub embed_animations: Vec<String>,
    /// Preferred auto-rigging backend provider ("builtin", "blender"). Defaults to None ("builtin").
    pub preferred_provider: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct HumanAutoRigSpec {
    #[serde(with = "bytes_serde")]
    input_mesh: Bytes,
    #[serde(default = "default_rig_preset")]
    rig_preset: String,
    #[serde(default = "default_embed_animations")]
    embed_animations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preferred_provider: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct BinaryAutoRigSpec {
    #[serde(with = "bytes_serde")]
    input_mesh: Bytes,
    #[serde(default = "default_rig_preset")]
    rig_preset: String,
    #[serde(default = "default_embed_animations")]
    embed_animations: Vec<String>,
    #[serde(default)]
    preferred_provider: Option<String>,
}

impl From<&AutoRigSpec> for HumanAutoRigSpec {
    fn from(s: &AutoRigSpec) -> Self {
        Self {
            input_mesh: s.input_mesh.clone(),
            rig_preset: s.rig_preset.clone(),
            embed_animations: s.embed_animations.clone(),
            preferred_provider: s.preferred_provider.clone(),
        }
    }
}

impl From<HumanAutoRigSpec> for AutoRigSpec {
    fn from(s: HumanAutoRigSpec) -> Self {
        Self {
            input_mesh: s.input_mesh,
            rig_preset: s.rig_preset,
            embed_animations: s.embed_animations,
            preferred_provider: s.preferred_provider,
        }
    }
}

impl From<&AutoRigSpec> for BinaryAutoRigSpec {
    fn from(s: &AutoRigSpec) -> Self {
        Self {
            input_mesh: s.input_mesh.clone(),
            rig_preset: s.rig_preset.clone(),
            embed_animations: s.embed_animations.clone(),
            preferred_provider: s.preferred_provider.clone(),
        }
    }
}

impl From<BinaryAutoRigSpec> for AutoRigSpec {
    fn from(s: BinaryAutoRigSpec) -> Self {
        Self {
            input_mesh: s.input_mesh,
            rig_preset: s.rig_preset,
            embed_animations: s.embed_animations,
            preferred_provider: s.preferred_provider,
        }
    }
}

impl Serialize for AutoRigSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanAutoRigSpec::from(self).serialize(serializer)
        } else {
            BinaryAutoRigSpec::from(self).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for AutoRigSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanAutoRigSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryAutoRigSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

impl AutoRigSpec {
    /// Creates an auto-rig spec with the input mesh and specified rig preset.
    pub fn new(input_mesh: impl Into<Bytes>, rig_preset: impl Into<String>) -> Self {
        Self {
            input_mesh: input_mesh.into(),
            rig_preset: rig_preset.into(),
            embed_animations: default_embed_animations(),
            preferred_provider: None,
        }
    }

    /// Overrides the animation clips to embed in the output asset.
    pub fn with_animations(mut self, animations: Vec<String>) -> Self {
        self.embed_animations = animations;
        self
    }

    /// Sets the preferred auto-rigging provider ("builtin", "blender").
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.preferred_provider = Some(provider.into());
        self
    }
}

/// Concrete computation payload and execution specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSpec {
    /// Direct command execution (e.g. `echo`, `python3`, CLI tools).
    Command {
        program: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        working_dir: Option<PathBuf>,
        stdin: Option<Vec<u8>>,
    },
    /// Shell script execution via system shell.
    ShellScript {
        script: String,
        interpreter: Option<String>,
        env: HashMap<String, String>,
    },
    /// Distributed Rust compilation job.
    RustCompilation {
        crate_name: String,
        /// Relative file paths mapped to file contents (e.g., "Cargo.toml", "src/lib.rs").
        source_files: HashMap<String, String>,
        /// Flags passed to compiler / cargo (e.g., ["--crate-type", "lib"]).
        compiler_flags: Vec<String>,
        target_dir: Option<PathBuf>,
    },
    /// GPU compute workload (matrix multiplication, hash calculations, kernel execution).
    GpuCompute {
        kernel_name: String,
        input_data: Vec<u8>,
        work_group_size: u32,
        simulated_matrix_dim: u32,
        compute_intensity: u32,
    },
    /// In-memory built-in test task for rapid verification.
    BuiltinTest {
        test_name: String,
        iterations: u32,
        duration_ms: u64,
        should_fail: bool,
        require_gpu: bool,
    },
    /// Local on-device 3D mesh generation from text prompt or input image.
    Generative3D(Generative3DSpec),
    /// Local automated skeleton rigging and animation synthesis for a 3D mesh.
    AutoRig(AutoRigSpec),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HumanTaskSpec {
    Command {
        #[serde(alias = "command")]
        program: String,
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
        #[serde(default)]
        working_dir: Option<PathBuf>,
        #[serde(default)]
        stdin: Option<Vec<u8>>,
    },
    ShellScript {
        script: String,
        #[serde(default)]
        interpreter: Option<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    RustCompilation {
        crate_name: String,
        source_files: HashMap<String, String>,
        #[serde(default, alias = "cargo_args")]
        compiler_flags: Vec<String>,
        #[serde(default)]
        target_dir: Option<PathBuf>,
    },
    GpuCompute {
        kernel_name: String,
        #[serde(default)]
        input_data: Vec<u8>,
        #[serde(default = "default_work_group_size")]
        work_group_size: u32,
        #[serde(default = "default_matrix_dim")]
        simulated_matrix_dim: u32,
        #[serde(default)]
        compute_intensity: u32,
    },
    BuiltinTest {
        #[serde(default = "default_test_name")]
        test_name: String,
        #[serde(default = "default_iterations")]
        iterations: u32,
        #[serde(default)]
        duration_ms: u64,
        #[serde(default)]
        should_fail: bool,
        #[serde(default)]
        require_gpu: bool,
    },
    #[serde(alias = "generative_3d", alias = "generative3d", alias = "3d", alias = "Generative3d")]
    Generative3D(Generative3DSpec),
    #[serde(alias = "auto_rig", alias = "autorig", alias = "rig", alias = "Auto_Rig")]
    AutoRig(AutoRigSpec),
}

#[derive(Serialize, Deserialize)]
enum BinaryTaskSpec {
    Command {
        program: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        working_dir: Option<PathBuf>,
        #[serde(default)]
        stdin: Option<Vec<u8>>,
    },
    ShellScript {
        script: String,
        interpreter: Option<String>,
        env: HashMap<String, String>,
    },
    RustCompilation {
        crate_name: String,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
        target_dir: Option<PathBuf>,
    },
    GpuCompute {
        kernel_name: String,
        #[serde(default)]
        input_data: Vec<u8>,
        work_group_size: u32,
        simulated_matrix_dim: u32,
        compute_intensity: u32,
    },
    BuiltinTest {
        test_name: String,
        iterations: u32,
        duration_ms: u64,
        should_fail: bool,
        require_gpu: bool,
    },
    Generative3D(Generative3DSpec),
    AutoRig(AutoRigSpec),
}

impl From<TaskSpec> for HumanTaskSpec {
    fn from(spec: TaskSpec) -> Self {
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => HumanTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => HumanTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => HumanTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => HumanTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => HumanTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
            TaskSpec::Generative3D(s) => HumanTaskSpec::Generative3D(s),
            TaskSpec::AutoRig(s) => HumanTaskSpec::AutoRig(s),
        }
    }
}

impl From<HumanTaskSpec> for TaskSpec {
    fn from(spec: HumanTaskSpec) -> Self {
        match spec {
            HumanTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            HumanTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            HumanTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            HumanTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            HumanTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
            HumanTaskSpec::Generative3D(s) => TaskSpec::Generative3D(s),
            HumanTaskSpec::AutoRig(s) => TaskSpec::AutoRig(s),
        }
    }
}

impl From<TaskSpec> for BinaryTaskSpec {
    fn from(spec: TaskSpec) -> Self {
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => BinaryTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => BinaryTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => BinaryTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => BinaryTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => BinaryTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
            TaskSpec::Generative3D(s) => BinaryTaskSpec::Generative3D(s),
            TaskSpec::AutoRig(s) => BinaryTaskSpec::AutoRig(s),
        }
    }
}

impl From<BinaryTaskSpec> for TaskSpec {
    fn from(spec: BinaryTaskSpec) -> Self {
        match spec {
            BinaryTaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            },
            BinaryTaskSpec::ShellScript {
                script,
                interpreter,
                env,
            } => TaskSpec::ShellScript {
                script,
                interpreter,
                env,
            },
            BinaryTaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            } => TaskSpec::RustCompilation {
                crate_name,
                source_files,
                compiler_flags,
                target_dir,
            },
            BinaryTaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            } => TaskSpec::GpuCompute {
                kernel_name,
                input_data,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
            },
            BinaryTaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            },
            BinaryTaskSpec::Generative3D(s) => TaskSpec::Generative3D(s),
            BinaryTaskSpec::AutoRig(s) => TaskSpec::AutoRig(s),
        }
    }
}

impl Serialize for TaskSpec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanTaskSpec::from(self.clone()).serialize(serializer)
        } else {
            BinaryTaskSpec::from(self.clone()).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TaskSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanTaskSpec::deserialize(deserializer).map(Into::into)
        } else {
            BinaryTaskSpec::deserialize(deserializer).map(Into::into)
        }
    }
}

impl TaskSpec {
    pub fn new_command(program: impl Into<String>, args: Vec<String>) -> Self {
        Self::Command {
            program: program.into(),
            args,
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        }
    }

    pub fn command(program: impl Into<String>, args: Vec<String>) -> Self {
        Self::new_command(program, args)
    }

    pub fn new_shell_script(script: impl Into<String>) -> Self {
        Self::ShellScript {
            script: script.into(),
            interpreter: None,
            env: HashMap::new(),
        }
    }

    pub fn shell_script(script: impl Into<String>) -> Self {
        Self::new_shell_script(script)
    }

    pub fn new_rust_compilation(
        crate_name: impl Into<String>,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
    ) -> Self {
        Self::RustCompilation {
            crate_name: crate_name.into(),
            source_files,
            compiler_flags,
            target_dir: None,
        }
    }

    pub fn rust_compilation(
        crate_name: impl Into<String>,
        source_files: HashMap<String, String>,
        compiler_flags: Vec<String>,
    ) -> Self {
        Self::new_rust_compilation(crate_name, source_files, compiler_flags)
    }

    pub fn new_gpu_compute(kernel_name: impl Into<String>, simulated_matrix_dim: u32) -> Self {
        Self::GpuCompute {
            kernel_name: kernel_name.into(),
            input_data: Vec::new(),
            work_group_size: default_work_group_size(),
            simulated_matrix_dim,
            compute_intensity: 1,
        }
    }

    pub fn gpu_compute(kernel_name: impl Into<String>, simulated_matrix_dim: u32) -> Self {
        Self::new_gpu_compute(kernel_name, simulated_matrix_dim)
    }

    pub fn new_builtin_test(test_name: impl Into<String>, iterations: u32) -> Self {
        Self::BuiltinTest {
            test_name: test_name.into(),
            iterations,
            duration_ms: 0,
            should_fail: false,
            require_gpu: false,
        }
    }

    pub fn builtin_test(test_name: impl Into<String>, iterations: u32) -> Self {
        Self::new_builtin_test(test_name, iterations)
    }

    /// Creates a new Generative3D task specification from a text prompt.
    pub fn new_generative_3d(prompt: impl Into<String>) -> Self {
        Self::Generative3D(Generative3DSpec::new_prompt(prompt))
    }

    /// Alias for new_generative_3d.
    pub fn generative_3d(prompt: impl Into<String>) -> Self {
        Self::new_generative_3d(prompt)
    }

    /// Creates a new Generative3D task specification from an input image buffer.
    pub fn new_generative_3d_image(image_data: impl Into<Bytes>) -> Self {
        Self::Generative3D(Generative3DSpec::new_image(image_data))
    }

    /// Creates a new AutoRig task specification from input mesh bytes and a rig preset.
    pub fn new_auto_rig(input_mesh: impl Into<Bytes>, rig_preset: impl Into<String>) -> Self {
        Self::AutoRig(AutoRigSpec::new(input_mesh, rig_preset))
    }

    /// Alias for new_auto_rig.
    pub fn auto_rig(input_mesh: impl Into<Bytes>, rig_preset: impl Into<String>) -> Self {
        Self::new_auto_rig(input_mesh, rig_preset)
    }

    /// Returns the static string descriptor of the task variant type.
    pub fn task_type(&self) -> &'static str {
        match self {
            TaskSpec::Command { .. } => "Command",
            TaskSpec::ShellScript { .. } => "ShellScript",
            TaskSpec::RustCompilation { .. } => "RustCompilation",
            TaskSpec::GpuCompute { .. } => "GpuCompute",
            TaskSpec::BuiltinTest { .. } => "BuiltinTest",
            TaskSpec::Generative3D(_) => "Generative3D",
            TaskSpec::AutoRig(_) => "AutoRig",
        }
    }

    /// Validates internal consistency of the task specification.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            TaskSpec::Command { program, .. } => {
                if program.trim().is_empty() {
                    return Err("Command program cannot be empty".to_string());
                }
            }
            TaskSpec::ShellScript { script, .. } => {
                if script.trim().is_empty() {
                    return Err("Shell script cannot be empty".to_string());
                }
            }
            TaskSpec::RustCompilation {
                crate_name,
                source_files,
                ..
            } => {
                if crate_name.trim().is_empty() {
                    return Err("Rust compilation crate_name cannot be empty".to_string());
                }
                if source_files.is_empty() {
                    return Err("Rust compilation requires at least one source file".to_string());
                }
            }
            TaskSpec::GpuCompute { kernel_name, .. } => {
                if kernel_name.trim().is_empty() {
                    return Err("GPU kernel_name cannot be empty".to_string());
                }
            }
            TaskSpec::BuiltinTest { .. } => {}
            TaskSpec::Generative3D(spec) => {
                let has_prompt = spec.prompt.as_ref().map(|p| !p.trim().is_empty()).unwrap_or(false);
                let has_image = spec.image_data.as_ref().map(|img| !img.is_empty()).unwrap_or(false);
                if !has_prompt && !has_image {
                    return Err("Generative3D requires at least a non-empty prompt or non-empty image_data".to_string());
                }
                if spec.quality.trim().is_empty() {
                    return Err("Generative3D quality cannot be empty".to_string());
                }
            }
            TaskSpec::AutoRig(spec) => {
                if spec.input_mesh.is_empty() {
                    return Err("AutoRig input_mesh cannot be empty".to_string());
                }
                if spec.rig_preset.trim().is_empty() {
                    return Err("AutoRig rig_preset cannot be empty".to_string());
                }
            }
        }
        Ok(())
    }

    /// Returns true if this specification inherently requires GPU execution.
    pub fn requires_gpu(&self) -> bool {
        match self {
            TaskSpec::GpuCompute { .. } => true,
            TaskSpec::BuiltinTest { require_gpu, .. } => *require_gpu,
            TaskSpec::Generative3D(spec) => {
                // Procedural SDF engine runs on CPU; neural or auto engines require GPU
                match spec.engine_preference.as_deref() {
                    Some("sdf") | Some("procedural") => false,
                    _ => true,
                }
            }
            TaskSpec::AutoRig(_) => false,
            _ => false,
        }
    }
}

/// Incremental computational snapshot emitted by a Worker during execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointData {
    pub sequence: u64,
    pub delta_state: Bytes,
    pub timestamp: u64,
}

impl CheckpointData {
    /// Creates a new CheckpointData with sequence, delta state, and current UNIX epoch timestamp.
    pub fn new(sequence: u64, delta_state: impl Into<Bytes>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            sequence,
            delta_state: delta_state.into(),
            timestamp,
        }
    }
}

/// Maximum allowable size for a single staged input file (32 MiB).
pub const MAX_STAGED_FILE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum allowable aggregate size for all staged input files in a task (32 MiB).
pub const MAX_AGGREGATE_STAGED_BYTES: usize = 32 * 1024 * 1024;

/// Maximum allowable size for a single output artifact captured from sandbox (10 MiB).
pub const MAX_OUTPUT_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;

/// Maximum allowable aggregate size for all output artifacts captured in a task (32 MiB).
pub const MAX_AGGREGATE_ARTIFACT_BYTES: usize = 32 * 1024 * 1024;

/// Windows reserved DOS device names that cannot be created or accessed as files.
pub const WINDOWS_RESERVED_DEVICE_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Computes SHA-256 digest over byte slice using ring.
pub fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

/// Encodes byte slice into lowercase hexadecimal string without external dependencies.
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Validates and normalizes a relative sandbox path.
///
/// Enforces:
/// 1. Rejection of embedded null bytes (`\0`).
/// 2. Normalization of Windows backslashes `\` to canonical forward slashes `/`.
/// 3. Rejection of empty or root paths (`""`, `"/"`).
/// 4. Rejection of Unix absolute paths (`/etc/passwd`).
/// 5. Rejection of Windows drive letters (`C:\`, `C:foo.txt`).
/// 6. Rejection of Windows UNC paths (`\\server\share`).
/// 7. Rejection of parent directory traversal (`..`).
/// 8. Rejection of colons or alternate data streams (`:`).
/// 9. Rejection of Windows DOS reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1..9`, `LPT1..9`).
///
/// Returns the canonical forward-slash separated relative path.
pub fn validate_relative_sandbox_path(raw: &str) -> Result<String, String> {
    if raw.is_empty() {
        return Err("Path cannot be empty".to_string());
    }
    if raw.contains('\0') {
        return Err("Path contains prohibited null byte".to_string());
    }
    if raw.starts_with('/') || raw.starts_with('\\') {
        return Err(format!("Absolute paths are prohibited: '{raw}'"));
    }
    if raw.contains(':') {
        return Err(format!("Colons and drive letters are prohibited: '{raw}'"));
    }

    let normalized = raw.replace('\\', "/");
    let trimmed = normalized.trim();
    if trimmed.is_empty() {
        return Err("Path cannot be empty".to_string());
    }
    if trimmed.starts_with('/') {
        return Err(format!("Absolute paths are prohibited: '{raw}'"));
    }
    if trimmed.starts_with("//") {
        return Err(format!("UNC prefix is prohibited: '{raw}'"));
    }

    let mut segments = Vec::new();
    for seg in trimmed.split('/') {
        let seg_trimmed = seg.trim();
        if seg_trimmed.is_empty() || seg_trimmed == "." {
            continue;
        }
        if seg_trimmed == ".." {
            return Err(format!(
                "Parent directory traversal ('..') is prohibited: '{raw}'"
            ));
        }
        let cleaned = seg_trimmed.trim_end_matches([' ', '.']);
        if cleaned.is_empty() {
            return Err(format!(
                "Illegal path component '{seg_trimmed}' consisting only of dots/spaces: '{raw}'"
            ));
        }
        let base_name = cleaned
            .split('.')
            .next()
            .unwrap_or(cleaned)
            .trim_end_matches([' ', '.'])
            .to_ascii_uppercase();
        if WINDOWS_RESERVED_DEVICE_NAMES.contains(&base_name.as_str()) {
            return Err(format!(
                "Windows reserved device name '{seg_trimmed}' is prohibited: '{raw}'"
            ));
        }
        segments.push(seg_trimmed);
    }

    if segments.is_empty() {
        return Err(format!(
            "Path resolved to empty after normalization: '{raw}'"
        ));
    }

    Ok(segments.join("/"))
}

/// Staged input file materialized into worker's task sandbox prior to execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedFile {
    /// Relative path within sandbox (canonical forward-slash delimited, e.g. "data/input.csv").
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// SHA-256 content digest.
    pub sha256: [u8; 32],
    /// Unix executable permission flag (0o755 if true, 0o644 if false).
    #[serde(default)]
    pub executable: bool,
    /// File payload bytes.
    pub data: Bytes,
}

impl StagedFile {
    pub fn new(path: impl Into<String>, data: impl Into<Bytes>, executable: bool) -> Self {
        let path = path.into();
        let data = data.into();
        let size = data.len() as u64;
        let sha256 = sha256_digest(data.as_bytes());
        Self {
            path,
            size,
            sha256,
            executable,
            data,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        if self.data.len() as u64 != self.size {
            return false;
        }
        sha256_digest(self.data.as_bytes()) == self.sha256
    }

    pub fn sha256_hex(&self) -> String {
        hex_encode(&self.sha256)
    }
}

/// Output artifact collected from worker's task sandbox upon task completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputArtifact {
    /// Relative path within sandbox (e.g. "model/weights.bin").
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// SHA-256 content digest.
    pub sha256: [u8; 32],
    /// File payload bytes.
    #[serde(alias = "content")]
    pub data: Bytes,
}

impl OutputArtifact {
    pub fn new(path: impl Into<String>, data: impl Into<Bytes>) -> Self {
        let path = path.into();
        let data = data.into();
        let size = data.len() as u64;
        let sha256 = sha256_digest(data.as_bytes());
        Self {
            path,
            size,
            sha256,
            data,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        if self.data.len() as u64 != self.size {
            return false;
        }
        sha256_digest(self.data.as_bytes()) == self.sha256
    }

    pub fn sha256_hex(&self) -> String {
        hex_encode(&self.sha256)
    }

    #[inline]
    pub fn content(&self) -> &Bytes {
        &self.data
    }
}

/// The top-level task representation submitted to Master and dispatched to Workers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    /// Unique task ID.
    pub id: TaskId,
    /// Computation payload.
    pub spec: TaskSpec,
    /// Resource requirements and timeouts.
    pub requirements: TaskRequirements,
    /// UTC timestamp of creation (seconds since UNIX epoch).
    pub created_at_utc: u64,
    /// User-defined tags or labels.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Latest checkpoint data for delta resumption.
    #[serde(default)]
    pub latest_checkpoint: Option<CheckpointData>,
    /// Staged input files to be placed into the task sandbox prior to execution.
    #[serde(default)]
    pub staged_files: Vec<StagedFile>,
    /// Declared output artifact paths to be captured from sandbox upon completion.
    #[serde(default)]
    pub expected_outputs: Vec<String>,
}

impl Task {
    /// Creates a new Task with generated TaskId and current timestamp.
    pub fn new(spec: TaskSpec, requirements: TaskRequirements) -> Self {
        let created_at_utc = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            id: TaskId::new(),
            spec,
            requirements,
            created_at_utc,
            tags: Vec::new(),
            latest_checkpoint: None,
            staged_files: Vec::new(),
            expected_outputs: Vec::new(),
        }
    }

    /// Builder method to attach tags.
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Builder method to attach latest checkpoint data.
    pub fn with_checkpoint(mut self, checkpoint: CheckpointData) -> Self {
        self.latest_checkpoint = Some(checkpoint);
        self
    }

    /// Builder method to attach staged input files.
    pub fn with_staged_files(mut self, staged_files: Vec<StagedFile>) -> Self {
        self.staged_files = staged_files;
        self
    }

    /// Builder method to attach a single staged input file.
    pub fn with_staged_file(mut self, file: StagedFile) -> Self {
        self.staged_files.push(file);
        self
    }

    /// Builder method to attach expected output artifact paths.
    pub fn with_expected_outputs(mut self, expected_outputs: Vec<String>) -> Self {
        self.expected_outputs = expected_outputs;
        self
    }

    /// Builder method to attach a single expected output artifact path.
    pub fn with_expected_output(mut self, path: impl Into<String>) -> Self {
        self.expected_outputs.push(path.into());
        self
    }

    /// Mutates the task to record an updated checkpoint snapshot.
    pub fn update_checkpoint(&mut self, checkpoint: CheckpointData) {
        self.latest_checkpoint = Some(checkpoint);
    }

    /// Validates the task and checks for specification/requirement mismatches.
    pub fn validate(&self) -> Result<(), String> {
        self.spec.validate()?;
        self.requirements.validate()?;

        if self.spec.requires_gpu() && !self.requirements.gpu_required {
            return Err(
                "TaskSpec requires GPU, but TaskRequirements.gpu_required is false".to_string(),
            );
        }

        self.placement()?;

        let mut total_staged: u64 = 0;
        for file in &self.staged_files {
            if file.data.len() as u64 != file.size {
                return Err(format!(
                    "Staged file '{}' payload length {} does not match declared size {}",
                    file.path,
                    file.data.len(),
                    file.size
                ));
            }
            if file.size > MAX_STAGED_FILE_BYTES as u64 {
                return Err(format!(
                    "Staged file '{}' size {} exceeds single file limit of 32 MiB",
                    file.path, file.size
                ));
            }
            total_staged = total_staged.saturating_add(file.size);
            if total_staged > MAX_AGGREGATE_STAGED_BYTES as u64 {
                return Err("Aggregate staged files size exceeds limit of 32 MiB".to_string());
            }
            validate_relative_sandbox_path(&file.path)?;
        }

        for output in &self.expected_outputs {
            validate_relative_sandbox_path(output)?;
        }

        Ok(())
    }

    /// The placement constraints in this task's `require:` tags (see [`crate::placement`]).
    pub fn placement(&self) -> Result<crate::placement::Placement, String> {
        crate::placement::Placement::from_task_tags(&self.tags)
    }

    /// Builder method to add placement constraints to the task's tags.
    pub fn with_placement(mut self, placement: &crate::placement::Placement) -> Self {
        self.tags.extend(placement.to_task_tags());
        self
    }
}

/// Execution outcome returned by a Worker upon task completion or termination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    /// Worker UUID that executed the task.
    pub worker_id: Uuid,
    /// Task ID that was executed.
    pub task_id: TaskId,
    /// Process exit code (0 indicates success).
    pub exit_code: i32,
    /// Captured standard output.
    pub stdout: Bytes,
    /// Captured standard error.
    pub stderr: Bytes,
    /// Execution duration in milliseconds.
    pub execution_time_ms: u64,
    /// Whether the task was executed on a physical or simulated GPU.
    pub is_gpu_executed: bool,
    /// Hardware device name or compute engine descriptor (Requirement R4).
    pub device_name: Option<String>,
    /// Optional error message if execution or sandboxing encountered an error.
    pub error: Option<String>,
    /// Optional zero-copy binary asset payload (e.g. glTF 2.0 .glb model).
    pub asset_bytes: Option<Bytes>,
    /// Captured output artifacts extracted from sandbox.
    pub artifacts: Vec<OutputArtifact>,
}

#[derive(Serialize, Deserialize)]
struct HumanTaskResult {
    pub worker_id: Uuid,
    pub task_id: TaskId,
    pub exit_code: i32,
    #[serde(with = "bytes_serde")]
    pub stdout: Bytes,
    #[serde(with = "bytes_serde")]
    pub stderr: Bytes,
    pub execution_time_ms: u64,
    pub is_gpu_executed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, with = "opt_bytes_serde", skip_serializing_if = "Option::is_none")]
    pub asset_bytes: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<OutputArtifact>,
}

#[derive(Serialize, Deserialize)]
struct BinaryTaskResult {
    pub worker_id: Uuid,
    pub task_id: TaskId,
    pub exit_code: i32,
    #[serde(with = "bytes_serde")]
    pub stdout: Bytes,
    #[serde(with = "bytes_serde")]
    pub stderr: Bytes,
    pub execution_time_ms: u64,
    pub is_gpu_executed: bool,
    pub device_name: Option<String>,
    pub error: Option<String>,
    #[serde(default, with = "opt_bytes_serde")]
    pub asset_bytes: Option<Bytes>,
    #[serde(default)]
    pub artifacts: Vec<OutputArtifact>,
}

#[derive(Serialize, Deserialize)]
struct LegacyBinaryTaskResult {
    pub worker_id: Uuid,
    pub task_id: TaskId,
    pub exit_code: i32,
    #[serde(with = "bytes_serde")]
    pub stdout: Bytes,
    #[serde(with = "bytes_serde")]
    pub stderr: Bytes,
    pub execution_time_ms: u64,
    pub is_gpu_executed: bool,
    pub device_name: Option<String>,
    pub error: Option<String>,
}

impl From<LegacyBinaryTaskResult> for TaskResult {
    fn from(l: LegacyBinaryTaskResult) -> Self {
        Self {
            worker_id: l.worker_id,
            task_id: l.task_id,
            exit_code: l.exit_code,
            stdout: l.stdout,
            stderr: l.stderr,
            execution_time_ms: l.execution_time_ms,
            is_gpu_executed: l.is_gpu_executed,
            device_name: l.device_name,
            error: l.error,
            asset_bytes: None,
            artifacts: Vec::new(),
        }
    }
}

impl From<&TaskResult> for HumanTaskResult {
    fn from(r: &TaskResult) -> Self {
        Self {
            worker_id: r.worker_id,
            task_id: r.task_id,
            exit_code: r.exit_code,
            stdout: r.stdout.clone(),
            stderr: r.stderr.clone(),
            execution_time_ms: r.execution_time_ms,
            is_gpu_executed: r.is_gpu_executed,
            device_name: r.device_name.clone(),
            error: r.error.clone(),
            asset_bytes: r.asset_bytes.clone(),
            artifacts: r.artifacts.clone(),
        }
    }
}

impl From<HumanTaskResult> for TaskResult {
    fn from(h: HumanTaskResult) -> Self {
        Self {
            worker_id: h.worker_id,
            task_id: h.task_id,
            exit_code: h.exit_code,
            stdout: h.stdout,
            stderr: h.stderr,
            execution_time_ms: h.execution_time_ms,
            is_gpu_executed: h.is_gpu_executed,
            device_name: h.device_name,
            error: h.error,
            asset_bytes: h.asset_bytes,
            artifacts: h.artifacts,
        }
    }
}

impl From<&TaskResult> for BinaryTaskResult {
    fn from(r: &TaskResult) -> Self {
        Self {
            worker_id: r.worker_id,
            task_id: r.task_id,
            exit_code: r.exit_code,
            stdout: r.stdout.clone(),
            stderr: r.stderr.clone(),
            execution_time_ms: r.execution_time_ms,
            is_gpu_executed: r.is_gpu_executed,
            device_name: r.device_name.clone(),
            error: r.error.clone(),
            asset_bytes: r.asset_bytes.clone(),
            artifacts: r.artifacts.clone(),
        }
    }
}

impl From<BinaryTaskResult> for TaskResult {
    fn from(b: BinaryTaskResult) -> Self {
        Self {
            worker_id: b.worker_id,
            task_id: b.task_id,
            exit_code: b.exit_code,
            stdout: b.stdout,
            stderr: b.stderr,
            execution_time_ms: b.execution_time_ms,
            is_gpu_executed: b.is_gpu_executed,
            device_name: b.device_name,
            error: b.error,
            asset_bytes: b.asset_bytes,
            artifacts: b.artifacts,
        }
    }
}

impl Serialize for TaskResult {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            HumanTaskResult::from(self).serialize(serializer)
        } else {
            BinaryTaskResult::from(self).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for TaskResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            HumanTaskResult::deserialize(deserializer).map(Into::into)
        } else {
            BinaryTaskResult::deserialize(deserializer).map(Into::into)
        }
    }
}

impl TaskResult {
    /// Returns true if execution completed with zero exit code and no framework error.
    #[inline]
    pub fn is_success(&self) -> bool {
        self.exit_code == 0 && self.error.is_none()
    }

    /// Sets the hardware device name or compute engine descriptor.
    pub fn with_device_name(mut self, device_name: impl Into<String>) -> Self {
        self.device_name = Some(device_name.into());
        self
    }

    /// Attaches an asset binary payload (e.g. generated .glb file) to the result.
    pub fn with_asset_bytes(mut self, asset_bytes: impl Into<Bytes>) -> Self {
        self.asset_bytes = Some(asset_bytes.into());
        self
    }

    /// Sets or clears the asset binary payload.
    pub fn set_asset_bytes(&mut self, asset_bytes: Option<Bytes>) {
        self.asset_bytes = asset_bytes;
    }

    /// Returns a reference to the asset binary payload if present.
    pub fn asset_bytes(&self) -> Option<&Bytes> {
        self.asset_bytes.as_ref()
    }

    /// Returns stdout rendered as a string slice (lossy UTF-8 conversion if necessary).
    #[inline]
    pub fn stdout_str(&self) -> std::borrow::Cow<'_, str> {
        self.stdout.to_string_lossy()
    }

    /// Returns stderr rendered as a string slice (lossy UTF-8 conversion if necessary).
    #[inline]
    pub fn stderr_str(&self) -> std::borrow::Cow<'_, str> {
        self.stderr.to_string_lossy()
    }

    /// Convenience constructor for successful execution.
    pub fn success(
        worker_id: Uuid,
        task_id: TaskId,
        stdout: impl Into<Bytes>,
        execution_time_ms: u64,
        is_gpu_executed: bool,
    ) -> Self {
        Self {
            worker_id,
            task_id,
            exit_code: 0,
            stdout: stdout.into(),
            stderr: Bytes::new(),
            execution_time_ms,
            is_gpu_executed,
            device_name: None,
            error: None,
            asset_bytes: None,
            artifacts: Vec::new(),
        }
    }

    /// Convenience constructor for failed execution.
    pub fn failure(
        worker_id: Uuid,
        task_id: TaskId,
        exit_code: i32,
        stdout: impl Into<Bytes>,
        stderr: impl Into<Bytes>,
        execution_time_ms: u64,
        error: Option<String>,
    ) -> Self {
        Self {
            worker_id,
            task_id,
            exit_code,
            stdout: stdout.into(),
            stderr: stderr.into(),
            execution_time_ms,
            is_gpu_executed: false,
            device_name: None,
            error,
            asset_bytes: None,
            artifacts: Vec::new(),
        }
    }

    /// Builder method to attach captured output artifacts.
    pub fn with_artifacts(mut self, artifacts: Vec<OutputArtifact>) -> Self {
        self.artifacts = artifacts;
        self
    }

    /// Builder method to attach a single output artifact.
    pub fn with_artifact(mut self, artifact: OutputArtifact) -> Self {
        self.artifacts.push(artifact);
        self
    }

    /// Deserializes a `TaskResult` from bincode bytes with legacy binary schema fallback.
    pub fn from_bincode_bytes(bytes: &[u8]) -> Result<Self, bincode::Error> {
        match bincode::deserialize::<BinaryTaskResult>(bytes) {
            Ok(b) => Ok(b.into()),
            Err(_) => {
                let legacy: LegacyBinaryTaskResult = bincode::deserialize(bytes)?;
                Ok(legacy.into())
            }
        }
    }
}

/// Finite State Machine representation of a task's lifecycle on the Master.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Submitted to Master and waiting in queue for available eligible worker.
    Queued,
    /// Selected by scheduler and dispatched to a worker.
    Assigned,
    /// Worker has acknowledged receipt and started execution.
    Running,
    /// Successfully finished execution (exit code 0).
    Completed,
    /// Execution failed (non-zero exit code or process error).
    Failed,
    /// Failed or orphaned by worker disconnect; re-queued for retry.
    Retried,
    /// Execution exceeded timeout threshold and was cancelled.
    TimedOut,
    /// Explicitly cancelled by user or client RPC.
    Cancelled,
}

impl TaskStatus {
    /// Returns true if this state is terminal (no further transitions possible).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::TimedOut
                | TaskStatus::Cancelled
        )
    }

    /// Returns true if the task is actively assigned or running on a worker.
    pub fn is_active(&self) -> bool {
        matches!(self, TaskStatus::Assigned | TaskStatus::Running)
    }

    /// Enforces the valid state transitions of the Master FSM.
    pub fn can_transition_to(&self, next: TaskStatus) -> bool {
        match self {
            TaskStatus::Queued => matches!(next, TaskStatus::Assigned | TaskStatus::Cancelled),
            TaskStatus::Assigned => matches!(
                next,
                TaskStatus::Running
                    | TaskStatus::Queued
                    | TaskStatus::Retried
                    | TaskStatus::Failed
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Running => matches!(
                next,
                TaskStatus::Completed
                    | TaskStatus::Failed
                    | TaskStatus::Retried
                    | TaskStatus::TimedOut
                    | TaskStatus::Cancelled
            ),
            TaskStatus::Retried => matches!(next, TaskStatus::Queued | TaskStatus::Failed),
            // Terminal states cannot transition to anything
            TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::TimedOut
            | TaskStatus::Cancelled => false,
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// cp437/cp850 "dir" output with an accented byte (0x82 = e-acute), invalid as UTF-8.
    fn oem_output() -> Vec<u8> {
        let mut v = b"Volume in drive C is Syst".to_vec();
        v.push(0x82);
        v.extend_from_slice(b"me\r\n 12 File(s)\r\n");
        v
    }

    #[test]
    fn test_bytes_valid_utf8_views_are_whole_and_borrowed() {
        let b = Bytes::from("h\u{e9}llo w\u{f6}rld \u{1f980}");
        assert_eq!(b.as_str(), "h\u{e9}llo w\u{f6}rld \u{1f980}");
        assert_eq!(b.try_as_str(), Some("h\u{e9}llo w\u{f6}rld \u{1f980}"));
        assert!(matches!(b.to_string_lossy(), std::borrow::Cow::Borrowed(_)));
        assert_eq!(b.to_string(), "h\u{e9}llo w\u{f6}rld \u{1f980}");
        assert_eq!(Bytes::new().as_str(), "");
        assert_eq!(Bytes::new().try_as_str(), Some(""));
    }

    #[test]
    fn test_bytes_non_utf8_output_is_not_blanked() {
        let raw = oem_output();
        let b = Bytes::from(raw.clone());

        // The lossy accessor keeps everything, marking only the bad byte.
        let lossy = b.to_string_lossy();
        assert!(matches!(lossy, std::borrow::Cow::Owned(_)));
        assert!(lossy.contains("Volume in drive C is Syst\u{fffd}me"));
        assert!(lossy.contains("12 File(s)"));
        assert_eq!(
            b.to_string(),
            lossy,
            "Display must not blank invalid output either"
        );

        // The borrowed view keeps the valid prefix instead of discarding the buffer.
        assert_eq!(b.as_str(), "Volume in drive C is Syst");
        assert_eq!(b.try_as_str(), None);
        // The raw bytes are untouched.
        assert_eq!(b.as_bytes(), raw.as_slice());
    }

    #[test]
    fn test_bytes_truncated_multibyte_char_keeps_everything_before_it() {
        // A byte cap that lands inside a 4-byte character (the runner truncates at a byte limit).
        let mut raw = b"first line\nsecond line\n".to_vec();
        raw.extend_from_slice(&"\u{1f980}".as_bytes()[..2]);
        let b = Bytes::from(raw);
        assert_eq!(b.as_str(), "first line\nsecond line\n");
        assert_eq!(b.to_string_lossy(), "first line\nsecond line\n\u{fffd}");
        assert_eq!(b.try_as_str(), None);

        // Invalid first byte: borrowed view is empty, lossy view is not.
        let b = Bytes::from(vec![0xFF, 0xFE, b'o', b'k']);
        assert_eq!(b.as_str(), "");
        assert_eq!(b.to_string_lossy(), "\u{fffd}\u{fffd}ok");
    }

    #[test]
    fn test_bytes_string_equality_is_byte_exact() {
        let invalid = Bytes::from(vec![0xFF, 0xFE]);
        // Invalid UTF-8 used to compare equal to "" because as_str() blanked it.
        let (empty_str, empty_string) = ("", String::new());
        assert!(invalid != empty_str);
        assert!(invalid != *empty_str);
        assert!(invalid != empty_string);
        assert!(empty_str != invalid);
        assert!(empty_string != invalid);

        let partly = Bytes::from(b"abc\xFFdef".to_vec());
        assert!(
            partly != "abc",
            "prefix view must not make a different buffer equal"
        );
        assert!(partly != "abcdef");

        let ok = Bytes::from("same");
        let owned = String::from("same");
        assert!(ok == "same" && ok == *"same" && ok == owned);
        assert!("same" == ok && owned == ok);
        assert!(ok != "other");
    }

    #[test]
    fn test_task_result_stdout_str_uses_lossy_accessor_for_non_utf8() {
        let mut result = TaskResult::success(
            Uuid::new_v4(),
            TaskId::new(),
            Bytes::from(oem_output()),
            1,
            false,
        );
        result.stderr = Bytes::from(vec![b'e', 0x80, b'r']);
        assert!(result.stdout_str().contains("Syst\u{fffd}me"));
        assert!(result.stdout_str().contains("12 File(s)"));
        assert_eq!(result.stderr_str(), "e\u{fffd}r");

        // Valid output is still borrowed, not copied.
        let ok = TaskResult::success(Uuid::new_v4(), TaskId::new(), Bytes::from("fine"), 1, false);
        assert!(matches!(
            ok.stdout_str(),
            std::borrow::Cow::Borrowed("fine")
        ));
    }

    #[test]
    fn test_bytes_non_utf8_survives_both_wire_codecs() {
        let b = Bytes::from(oem_output());
        let bin: Bytes = bincode::deserialize(&bincode::serialize(&b).unwrap()).unwrap();
        assert_eq!(bin.as_bytes(), b.as_bytes());
        let json: Bytes = serde_json::from_slice(&serde_json::to_vec(&b).unwrap()).unwrap();
        assert_eq!(json.as_bytes(), b.as_bytes());
    }

    #[test]
    fn test_task_id_serde_transparent() {
        let raw_uuid = Uuid::new_v4();
        let task_id = TaskId::from_uuid(raw_uuid);

        let json = serde_json::to_string(&task_id).unwrap();
        // Because of #[serde(transparent)], it should be serialized directly as a quoted UUID string
        assert_eq!(json, format!("\"{}\"", raw_uuid));

        let deserialized: TaskId = serde_json::from_str(&json).unwrap();
        assert_eq!(task_id, deserialized);
        assert_eq!(task_id.to_string(), raw_uuid.to_string());
    }

    #[test]
    fn test_task_spec_validation() {
        // 1. Empty command
        let invalid_cmd = TaskSpec::Command {
            program: "".into(),
            args: vec![],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert!(invalid_cmd.validate().is_err());

        // 2. Valid command
        let valid_cmd = TaskSpec::Command {
            program: "ls".into(),
            args: vec!["-la".into()],
            env: HashMap::new(),
            working_dir: None,
            stdin: None,
        };
        assert!(valid_cmd.validate().is_ok());

        // 3. Empty Rust compilation crate name
        let invalid_rust = TaskSpec::RustCompilation {
            crate_name: "".into(),
            source_files: HashMap::new(),
            compiler_flags: vec![],
            target_dir: None,
        };
        assert!(invalid_rust.validate().is_err());

        // 4. Rust compilation without source files
        let no_sources_rust = TaskSpec::RustCompilation {
            crate_name: "my_crate".into(),
            source_files: HashMap::new(),
            compiler_flags: vec![],
            target_dir: None,
        };
        assert!(no_sources_rust.validate().is_err());

        // 5. Valid Rust compilation
        let mut sources = HashMap::new();
        sources.insert("Cargo.toml".into(), "[package]\nname = \"foo\"".into());
        let valid_rust = TaskSpec::RustCompilation {
            crate_name: "my_crate".into(),
            source_files: sources,
            compiler_flags: vec!["--release".into()],
            target_dir: None,
        };
        assert!(valid_rust.validate().is_ok());
    }

    #[test]
    fn test_task_gpu_mismatch_validation() {
        // Spec requires GPU, but requirements state gpu_required = false
        let gpu_spec = TaskSpec::GpuCompute {
            kernel_name: "matrix_mult".into(),
            input_data: vec![1, 2, 3],
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 10,
        };
        let bad_requirements = TaskRequirements::generic(1, 60);

        let task = Task::new(gpu_spec.clone(), bad_requirements);
        assert!(
            task.validate().is_err(),
            "Should error when GPU spec lacks gpu_required"
        );

        let good_requirements = TaskRequirements::gpu(60);
        let valid_task = Task::new(gpu_spec, good_requirements);
        assert!(valid_task.validate().is_ok());
    }

    #[test]
    fn test_task_spec_serde_aliases_and_defaults() {
        // Test "command" alias deserializes into `program`
        let json_cmd = r#"{"type":"Command","command":"echo","args":["hi"]}"#;
        let spec: TaskSpec = serde_json::from_str(json_cmd).unwrap();
        match spec {
            TaskSpec::Command {
                program,
                args,
                env,
                working_dir,
                stdin,
            } => {
                assert_eq!(program, "echo");
                assert_eq!(args, vec!["hi"]);
                assert!(env.is_empty());
                assert!(working_dir.is_none());
                assert!(stdin.is_none());
            }
            _ => panic!("Expected Command variant"),
        }

        // Test "cargo_args" alias deserializes into `compiler_flags`
        let json_rust = r#"{"type":"RustCompilation","crate_name":"foo","source_files":{"src/lib.rs":"fn f(){}"},"cargo_args":["--release"]}"#;
        let spec_rust: TaskSpec = serde_json::from_str(json_rust).unwrap();
        match spec_rust {
            TaskSpec::RustCompilation {
                crate_name,
                compiler_flags,
                target_dir,
                ..
            } => {
                assert_eq!(crate_name, "foo");
                assert_eq!(compiler_flags, vec!["--release"]);
                assert!(target_dir.is_none());
            }
            _ => panic!("Expected RustCompilation variant"),
        }

        // Test GpuCompute defaults
        let json_gpu = r#"{"type":"GpuCompute","kernel_name":"test"}"#;
        let spec_gpu: TaskSpec = serde_json::from_str(json_gpu).unwrap();
        match spec_gpu {
            TaskSpec::GpuCompute {
                kernel_name,
                work_group_size,
                simulated_matrix_dim,
                compute_intensity,
                ..
            } => {
                assert_eq!(kernel_name, "test");
                assert_eq!(work_group_size, 16);
                assert_eq!(simulated_matrix_dim, 64);
                assert_eq!(compute_intensity, 0);
            }
            _ => panic!("Expected GpuCompute variant"),
        }

        // Test BuiltinTest defaults
        let json_builtin = r#"{"type":"BuiltinTest"}"#;
        let spec_builtin: TaskSpec = serde_json::from_str(json_builtin).unwrap();
        match spec_builtin {
            TaskSpec::BuiltinTest {
                test_name,
                iterations,
                duration_ms,
                should_fail,
                require_gpu,
            } => {
                assert_eq!(test_name, "hash_compute");
                assert_eq!(iterations, 100_000);
                assert_eq!(duration_ms, 0);
                assert!(!should_fail);
                assert!(!require_gpu);
            }
            _ => panic!("Expected BuiltinTest variant"),
        }
    }

    #[test]
    fn test_task_status_transitions() {
        let q = TaskStatus::Queued;
        assert!(q.can_transition_to(TaskStatus::Assigned));
        assert!(q.can_transition_to(TaskStatus::Cancelled));
        assert!(!q.can_transition_to(TaskStatus::Running));
        assert!(!q.can_transition_to(TaskStatus::Completed));

        let a = TaskStatus::Assigned;
        assert!(a.can_transition_to(TaskStatus::Running));
        assert!(a.can_transition_to(TaskStatus::Retried));
        assert!(a.can_transition_to(TaskStatus::Failed));
        assert!(a.can_transition_to(TaskStatus::TimedOut));

        let r = TaskStatus::Running;
        assert!(r.can_transition_to(TaskStatus::Completed));
        assert!(r.can_transition_to(TaskStatus::Failed));
        assert!(r.can_transition_to(TaskStatus::TimedOut));
        assert!(!r.can_transition_to(TaskStatus::Assigned));

        let c = TaskStatus::Completed;
        assert!(c.is_terminal());
        assert!(!c.can_transition_to(TaskStatus::Queued));
        assert!(!c.can_transition_to(TaskStatus::Running));
    }

    #[test]
    fn test_task_result_helpers() {
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();

        let success = TaskResult::success(worker_id, task_id, "output", 150, true);
        assert!(success.is_success());
        assert!(success.is_gpu_executed);
        assert_eq!(success.exit_code, 0);

        let failure = TaskResult::failure(
            worker_id,
            task_id,
            1,
            "",
            "syntax error",
            200,
            Some("Failed execution".into()),
        );
        assert!(!failure.is_success());
        assert_eq!(failure.exit_code, 1);
    }

    #[test]
    fn test_task_serialization_roundtrip() {
        let spec = TaskSpec::ShellScript {
            script: "echo 'hello grid'".into(),
            interpreter: None,
            env: HashMap::new(),
        };
        let req = TaskRequirements::generic(2, 30);
        let task = Task::new(spec, req).with_tags(vec!["test".into(), "smoke".into()]);

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: Task = serde_json::from_str(&json).unwrap();

        assert_eq!(task, deserialized);
    }

    #[test]
    fn test_task_bincode_serialization_roundtrip() {
        let specs = vec![
            TaskSpec::new_command("cargo", vec!["build".into(), "--release".into()]),
            TaskSpec::new_shell_script("echo 'testing bincode'"),
            TaskSpec::RustCompilation {
                crate_name: "test_crate".into(),
                source_files: [("src/lib.rs".to_string(), "pub fn f() {}".to_string())]
                    .into_iter()
                    .collect(),
                compiler_flags: vec!["--crate-type".into(), "lib".into()],
                target_dir: None,
            },
            TaskSpec::GpuCompute {
                kernel_name: "gemm".into(),
                input_data: vec![1, 2, 3, 4, 5],
                work_group_size: 32,
                simulated_matrix_dim: 128,
                compute_intensity: 50,
            },
            TaskSpec::BuiltinTest {
                test_name: "quick_test".into(),
                iterations: 500,
                duration_ms: 10,
                should_fail: false,
                require_gpu: true,
            },
        ];

        for spec in specs {
            let req = TaskRequirements::generic(4, 60);
            let task = Task::new(spec, req).with_tags(vec!["bincode".into()]);

            let encoded = bincode::serialize(&task).expect("bincode serialization failed");
            let decoded: Task =
                bincode::deserialize(&encoded).expect("bincode deserialization failed");

            assert_eq!(task, decoded);
        }
    }

    #[test]
    fn test_task_requirements_max_retries() {
        let req_default = TaskRequirements::default();
        assert_eq!(req_default.max_retries, None);

        let req_custom = TaskRequirements::generic(2, 30).with_max_retries(5);
        assert_eq!(req_custom.max_retries, Some(5));

        // Test JSON roundtrip preserves max_retries
        let json = serde_json::to_string(&req_custom).unwrap();
        let decoded: TaskRequirements = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.max_retries, Some(5));

        // Test backward compatibility: JSON without max_retries field deserializes to None
        let legacy_json = r#"{"cpu_cores":1,"ram_mb":512,"gpu_required":false,"timeout_secs":60}"#;
        let decoded_legacy: TaskRequirements = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(decoded_legacy.max_retries, None);
    }

    #[test]
    fn test_task_checkpoint_serialization_roundtrip() {
        let checkpoint = CheckpointData::new(42, Bytes::copy_from_slice(b"delta_state_payload"));
        assert_eq!(checkpoint.sequence, 42);
        assert_eq!(checkpoint.delta_state.as_bytes(), b"delta_state_payload");

        let task = Task::new(
            TaskSpec::BuiltinTest {
                test_name: "test".into(),
                iterations: 100,
                duration_ms: 0,
                should_fail: false,
                require_gpu: false,
            },
            TaskRequirements::generic(1, 10),
        )
        .with_checkpoint(checkpoint.clone());

        assert_eq!(task.latest_checkpoint, Some(checkpoint.clone()));

        // JSON roundtrip
        let json = serde_json::to_string(&task).unwrap();
        let decoded_json: Task = serde_json::from_str(&json).unwrap();
        assert_eq!(task, decoded_json);
        assert_eq!(decoded_json.latest_checkpoint, Some(checkpoint.clone()));

        // Bincode roundtrip
        let bin = bincode::serialize(&task).unwrap();
        let decoded_bin: Task = bincode::deserialize(&bin).unwrap();
        assert_eq!(task, decoded_bin);
        assert_eq!(decoded_bin.latest_checkpoint, Some(checkpoint));
    }

    #[test]
    fn test_generative_3d_spec_json_roundtrip() {
        let spec = Generative3DSpec::new_prompt("a cybernetic owl")
            .with_quality("high")
            .with_engine("sdf")
            .with_export_texture(false);

        let task_spec = TaskSpec::Generative3D(spec);
        let json = serde_json::to_string(&task_spec).expect("JSON serialization failed");

        // Verify JSON contains expected tag and flattened fields
        assert!(json.contains("\"type\":\"Generative3D\""));
        assert!(json.contains("\"prompt\":\"a cybernetic owl\""));
        assert!(json.contains("\"quality\":\"high\""));
        assert!(json.contains("\"engine_preference\":\"sdf\""));
        assert!(json.contains("\"export_texture\":false"));

        let decoded: TaskSpec = serde_json::from_str(&json).expect("JSON deserialization failed");
        assert_eq!(task_spec, decoded);
    }

    #[test]
    fn test_generative_3d_spec_bincode_roundtrip() {
        let spec = Generative3DSpec::new_image(Bytes::copy_from_slice(b"\x89PNG\r\n\x1a\n"))
            .with_quality("fast");
        let task_spec = TaskSpec::Generative3D(spec);

        let encoded = bincode::serialize(&task_spec).expect("Bincode serialization failed");
        let decoded: TaskSpec = bincode::deserialize(&encoded).expect("Bincode deserialization failed");
        assert_eq!(task_spec, decoded);
    }

    #[test]
    fn test_generative_3d_aliases_and_defaults() {
        let json_minimal = r#"{"type":"3d","prompt":"fantasy sword"}"#;
        let decoded: TaskSpec = serde_json::from_str(json_minimal).expect("Alias deserialization failed");

        match decoded {
            TaskSpec::Generative3D(s) => {
                assert_eq!(s.prompt, Some("fantasy sword".to_string()));
                assert_eq!(s.quality, "standard");
                assert_eq!(s.export_texture, true);
                assert!(s.image_data.is_none());
                assert!(s.engine_preference.is_none());
            }
            _ => panic!("Expected Generative3D variant"),
        }
    }

    #[test]
    fn test_auto_rig_spec_serde_roundtrip() {
        let mesh_data = Bytes::copy_from_slice(b"glTF binary payload test");
        let spec = AutoRigSpec::new(mesh_data, "humanoid")
            .with_provider("builtin");

        let task_spec = TaskSpec::AutoRig(spec);

        // JSON roundtrip
        let json = serde_json::to_string(&task_spec).unwrap();
        assert!(json.contains("\"type\":\"AutoRig\""));
        assert!(json.contains("\"rig_preset\":\"humanoid\""));
        let decoded_json: TaskSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(task_spec, decoded_json);

        // Bincode roundtrip
        let bin = bincode::serialize(&task_spec).unwrap();
        let decoded_bin: TaskSpec = bincode::deserialize(&bin).unwrap();
        assert_eq!(task_spec, decoded_bin);
    }

    #[test]
    fn test_auto_rig_aliases_and_defaults() {
        let json = r#"{"type":"auto_rig","input_mesh":"Z2xURg=="}"#;
        let decoded: TaskSpec = serde_json::from_str(json).expect("Deserialization failed");
        match decoded {
            TaskSpec::AutoRig(s) => {
                assert_eq!(s.rig_preset, "biped");
                assert_eq!(s.embed_animations, vec!["idle", "walk", "turnaround"]);
                assert!(s.preferred_provider.is_none());
            }
            _ => panic!("Expected AutoRig variant"),
        }
    }

    #[test]
    fn test_task_spec_3d_validation() {
        // Valid prompt
        assert!(TaskSpec::new_generative_3d("valid prompt").validate().is_ok());

        // Valid image
        assert!(TaskSpec::new_generative_3d_image(Bytes::from_static(b"image")).validate().is_ok());

        // Invalid: empty prompt and no image
        let empty_prompt = TaskSpec::Generative3D(Generative3DSpec {
            prompt: Some("   ".to_string()),
            image_data: None,
            quality: "standard".to_string(),
            engine_preference: None,
            export_texture: true,
        });
        assert!(empty_prompt.validate().is_err());

        // Invalid: AutoRig empty mesh
        let empty_mesh = TaskSpec::AutoRig(AutoRigSpec::new(Bytes::new(), "biped"));
        assert!(empty_mesh.validate().is_err());

        // Invalid: AutoRig empty preset
        let empty_preset = TaskSpec::AutoRig(AutoRigSpec::new(Bytes::from_static(b"mesh"), "  "));
        assert!(empty_preset.validate().is_err());
    }

    #[test]
    fn test_task_spec_3d_requires_gpu() {
        // Standard / triposr engine requires GPU
        let gen_gpu = TaskSpec::new_generative_3d("mech");
        assert!(gen_gpu.requires_gpu());

        // Procedural SDF engine does NOT require GPU (CPU CI safe)
        let gen_cpu = TaskSpec::Generative3D(
            Generative3DSpec::new_prompt("cube").with_engine("sdf")
        );
        assert!(!gen_cpu.requires_gpu());

        // AutoRig does NOT require GPU
        let rig = TaskSpec::new_auto_rig(Bytes::from_static(b"mesh"), "biped");
        assert!(!rig.requires_gpu());
    }

    #[test]
    fn test_task_spec_task_type() {
        assert_eq!(TaskSpec::new_generative_3d("x").task_type(), "Generative3D");
        assert_eq!(TaskSpec::new_auto_rig(Bytes::new(), "biped").task_type(), "AutoRig");
        assert_eq!(TaskSpec::new_command("ls", vec![]).task_type(), "Command");
    }

    #[test]
    fn test_task_result_asset_bytes_roundtrip() {
        let wid = Uuid::new_v4();
        let tid = TaskId::new();
        let payload = Bytes::copy_from_slice(b"glTF 2.0 binary asset mock");

        let mut res = TaskResult::success(wid, tid, "3D generation complete", 250, true);
        res = res.with_asset_bytes(payload.clone());

        assert_eq!(res.asset_bytes(), Some(&payload));

        // JSON roundtrip
        let json = serde_json::to_string(&res).expect("TaskResult JSON serialize failed");
        let decoded_json: TaskResult = serde_json::from_str(&json).expect("TaskResult JSON deserialize failed");
        assert_eq!(res, decoded_json);

        // Bincode roundtrip
        let bin = bincode::serialize(&res).expect("TaskResult Bincode serialize failed");
        let decoded_bin: TaskResult = bincode::deserialize(&bin).expect("TaskResult Bincode deserialize failed");
        assert_eq!(res, decoded_bin);

        // Verify None case omits asset_bytes in JSON
        let res_none = TaskResult::success(wid, tid, "done", 50, false);
        let json_none = serde_json::to_string(&res_none).unwrap();
        assert!(!json_none.contains("\"asset_bytes\""));
    }

    #[test]
    fn test_task_result_asset_bytes_backward_compat() {
        let worker_id = Uuid::new_v4();
        let task_id = TaskId::new();

        // 1. TaskResult::success without asset_bytes
        let res = TaskResult::success(worker_id, task_id, "build success", 120, false);
        assert!(res.asset_bytes.is_none());

        // In JSON, asset_bytes = None must be omitted from output
        let json = serde_json::to_string(&res).expect("serialize legacy TaskResult");
        assert!(!json.contains("asset_bytes"), "JSON should skip asset_bytes when None: {json}");

        // 2. Deserializing legacy JSON (without asset_bytes field) must yield None
        let legacy_json = format!(
            r#"{{"worker_id":"{worker_id}","task_id":"{task_id}","exit_code":0,"stdout":"ok","stderr":"","execution_time_ms":100,"is_gpu_executed":false}}"#
        );
        let deser_legacy: TaskResult = serde_json::from_str(&legacy_json).expect("deserialize legacy json");
        assert_eq!(deser_legacy.asset_bytes, None);
        assert_eq!(deser_legacy.stdout.as_str(), "ok");
    }

    #[test]
    fn test_task_result_zero_copy_subslice_integrity() {
        // 1 MB binary payload
        let raw_payload: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let original_bytes = Bytes::copy_from_slice(&raw_payload);

        let res = TaskResult::success(Uuid::new_v4(), TaskId::new(), "", 10, false)
            .with_asset_bytes(original_bytes.clone());

        let asset = res.asset_bytes.as_ref().unwrap();
        assert_eq!(asset.len(), 1_000_000);

        // Subslice first 12 bytes (glTF header)
        let header = asset.slice(0..12);
        assert_eq!(header.len(), 12);
        assert_eq!(header.as_bytes(), &raw_payload[0..12]);

        // Subslice body (offset 12 to 1MB)
        let body = asset.slice(12..);
        assert_eq!(body.len(), 999_988);
        assert_eq!(body.as_bytes(), &raw_payload[12..]);

        // Zero copy verification: memory pointers point into the same allocation
        let base_ptr = asset.as_bytes().as_ptr();
        let body_ptr = body.as_bytes().as_ptr();
        assert_eq!(unsafe { base_ptr.add(12) }, body_ptr);
    }

    #[test]
    fn test_task_spec_conversion_parity() {
        let gen_spec = Generative3DSpec::new_prompt("cyberpunk character")
            .with_quality("standard")
            .with_engine("sdf");
        let rig_spec = AutoRigSpec::new(Bytes::copy_from_slice(b"dummy_mesh_geometry"), "biped");

        let specs = vec![TaskSpec::Generative3D(gen_spec), TaskSpec::AutoRig(rig_spec)];

        for spec in specs {
            // TaskSpec -> HumanTaskSpec -> TaskSpec parity
            let human: HumanTaskSpec = spec.clone().into();
            let from_human: TaskSpec = human.into();
            assert_eq!(spec, from_human, "HumanTaskSpec From conversion parity failed");

            // TaskSpec -> BinaryTaskSpec -> TaskSpec parity
            let binary: BinaryTaskSpec = spec.clone().into();
            let from_binary: TaskSpec = binary.into();
            assert_eq!(spec, from_binary, "BinaryTaskSpec From conversion parity failed");
        }
    }

    #[test]
    fn test_full_task_3d_roundtrip() {
        let gen_spec = Generative3DSpec::new_prompt("sci-fi hovercraft")
            .with_quality("high")
            .with_engine("auto");
        let spec = TaskSpec::Generative3D(gen_spec);
        let reqs = TaskRequirements::gpu(120);
        let task = Task::new(spec, reqs).with_tags(vec!["3d".into(), "production".into()]);

        // Validation must pass (Generative3D requires GPU, requirements state gpu_required = true)
        assert!(task.validate().is_ok());

        // JSON roundtrip
        let json = serde_json::to_string(&task).expect("serialize Task with Generative3D");
        let deser_task: Task = serde_json::from_str(&json).expect("deserialize Task with Generative3D");
        assert_eq!(task, deser_task);

        // Bincode roundtrip
        let bin = bincode::serialize(&task).expect("bincode serialize Task with Generative3D");
        let deser_bin: Task = bincode::deserialize(&bin).expect("bincode deserialize Task with Generative3D");
        assert_eq!(task, deser_bin);
    }
}
