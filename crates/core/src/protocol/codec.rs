use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::hmac;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{split, AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec::{Framed, FramedParts, FramedWrite, LengthDelimitedCodec};

use super::client_msg_mirrors::{
    BinaryClientMessage, HumanClientMessage, LegacyV2BinaryClientMessage,
    LegacyV5BinaryClientMessage, LegacyV6ToV8BinaryClientMessage, LegacyV9BinaryClientMessage,
};
use super::client_resp_mirrors::{
    serialize_client_response_with_version, BinaryClientResponse, HumanClientResponse,
    LegacyV2BinaryClientResponse, LegacyV3V4BinaryClientResponse,
};
use super::worker_master_mirrors::{
    master_message_for_peer, serialize_master_message_v9_bincode, BinaryInboundMessage,
    LegacyBinaryMasterMessage, LegacyBinaryWorkerMessage,
};
use super::{
    is_master_echo_acceptable, is_protocol_version_compatible, ClientMessage, ClientResponse,
    EndpointRole, InboundMessage, MasterMessage, ProtocolError, WireCodec, WorkerMessage,
    LENGTH_FIELD_BYTES, MAX_FRAME_SIZE, PROTOCOL_VERSION_CURRENT, PROTOCOL_VERSION_MIN_COMPATIBLE,
    WIRE_FORMAT_BINCODE, WIRE_FORMAT_ENCRYPTED, WIRE_FORMAT_JSON,
};

// Error texts of the record layer. They are fixed strings on purpose: callers log them, so they
// must never carry key material, nonces / sequence numbers or plaintext.
pub(super) const AEAD_ERR_TOO_SHORT: &str = "Encrypted frame too short to be an AEAD record";
pub(super) const AEAD_ERR_NOT_ENCRYPTED: &str =
    "Expected encrypted wire format (0x04) after the handshake";
pub(super) const AEAD_ERR_SEQUENCE: &str =
    "Anti-replay check failed: record out of sequence (replayed, reordered or dropped)";
pub(super) const AEAD_ERR_INTEGRITY: &str = "Ciphertext integrity verification failed";
pub(super) const AEAD_ERR_OVERFLOW: &str = "AEAD sequence counter overflow";
pub(super) const AEAD_ERR_CLOSED: &str = "AEAD session closed after an earlier record failure";

/// Bytes an AEAD record adds to the frame it carries: format byte, 8-byte sequence number and
/// 16-byte Poly1305 tag.
pub const AEAD_RECORD_OVERHEAD: usize = 1 + 8 + 16;

/// AEAD encryption session for sealing outbound frames (one direction of a connection).
///
/// Each record uses the nonce `0x00000000 || seq` (big endian) and authenticates its header as
/// AAD. `seq` starts at 0 and grows by one per record; it never wraps, so a nonce is never used
/// twice under one key.
pub struct AeadWriterSession {
    key: LessSafeKey,
    seq: u64,
}

impl AeadWriterSession {
    /// Creates a new writer session with a 32-byte symmetric key.
    pub fn new(write_key: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, write_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            key: LessSafeKey::new(unbound),
            seq: 0,
        }
    }

    /// A writer whose next record carries `seq` (for the counter-overflow tests).
    #[cfg(test)]
    pub(super) fn starting_at(write_key: &[u8; 32], seq: u64) -> Self {
        Self {
            seq,
            ..Self::new(write_key)
        }
    }

    /// Encrypts `plaintext` into a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte Poly1305 tag]`
    ///
    /// A record that would exceed [`MAX_FRAME_SIZE`] is refused without using up a sequence
    /// number, so the direction stays in sync. Once the counter is exhausted every call fails.
    pub fn encrypt_frame(&mut self, plaintext: &[u8]) -> Result<Bytes, ProtocolError> {
        let size = plaintext.len() + AEAD_RECORD_OVERHEAD;
        if size > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size,
                max: MAX_FRAME_SIZE,
            });
        }
        let seq = self.seq;
        // The counter only moves once the record is sealed; `u64::MAX` itself is never used.
        let next_seq = seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation(AEAD_ERR_OVERFLOW.into()))?;

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&seq.to_be_bytes());
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| ProtocolError::Violation("Invalid AEAD nonce".into()))?;

        let mut aad = [0u8; 9];
        aad[0] = WIRE_FORMAT_ENCRYPTED;
        aad[1..9].copy_from_slice(&seq.to_be_bytes());

        let mut in_out = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(nonce, Aad::from(&aad), &mut in_out)
            .map_err(|_| ProtocolError::Violation("AEAD encryption failure".into()))?;

        let mut frame = Vec::with_capacity(9 + in_out.len());
        frame.extend_from_slice(&aad);
        frame.extend_from_slice(&in_out);

        self.seq = next_seq;
        Ok(Bytes::from(frame))
    }

    /// Returns the next sequence number to be sent.
    pub fn current_seq(&self) -> u64 {
        self.seq
    }
}

/// AEAD decryption session for opening inbound frames (one direction of a connection).
///
/// It accepts exactly the next sequence number, so a replayed, reordered, dropped or injected
/// record is refused, and it fails closed: after the first refused record every later one is
/// refused too, even a genuine one.
pub struct AeadReaderSession {
    key: LessSafeKey,
    expected_seq: u64,
    /// Set by the first refused record.
    failed: bool,
}

impl AeadReaderSession {
    /// Creates a new reader session with a 32-byte symmetric key.
    pub fn new(read_key: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&CHACHA20_POLY1305, read_key)
            .expect("valid 32-byte key for ChaCha20-Poly1305");
        Self {
            key: LessSafeKey::new(unbound),
            expected_seq: 0,
            failed: false,
        }
    }

    /// A reader that expects `expected_seq` next (for the counter-overflow tests).
    #[cfg(test)]
    pub(super) fn starting_at(read_key: &[u8; 32], expected_seq: u64) -> Self {
        Self {
            expected_seq,
            ..Self::new(read_key)
        }
    }

    /// Decrypts a framed encrypted payload:
    /// `[0x04][seq: 8 bytes BE][ciphertext][16-byte tag]`
    /// and returns the decrypted plaintext bytes.
    pub fn decrypt_frame(&mut self, frame: &[u8]) -> Result<Bytes, ProtocolError> {
        if self.failed {
            return Err(ProtocolError::Violation(AEAD_ERR_CLOSED.into()));
        }
        let opened = self.open(frame);
        self.failed = opened.is_err();
        opened
    }

    fn open(&mut self, frame: &[u8]) -> Result<Bytes, ProtocolError> {
        if frame.len() < AEAD_RECORD_OVERHEAD {
            return Err(ProtocolError::Violation(AEAD_ERR_TOO_SHORT.into()));
        }

        if frame[0] != WIRE_FORMAT_ENCRYPTED {
            return Err(ProtocolError::Violation(AEAD_ERR_NOT_ENCRYPTED.into()));
        }

        let seq = u64::from_be_bytes(frame[1..9].try_into().unwrap());
        if seq != self.expected_seq {
            return Err(ProtocolError::Violation(AEAD_ERR_SEQUENCE.into()));
        }

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[4..12].copy_from_slice(&seq.to_be_bytes());
        let nonce = Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|_| ProtocolError::Violation("Invalid AEAD nonce".into()))?;

        let aad = &frame[0..9];
        let mut in_out = frame[9..].to_vec();

        let plaintext_slice = self
            .key
            .open_in_place(nonce, Aad::from(aad), &mut in_out)
            .map_err(|_| ProtocolError::Violation(AEAD_ERR_INTEGRITY.into()))?;

        let plaintext = Bytes::copy_from_slice(plaintext_slice);
        self.expected_seq = self
            .expected_seq
            .checked_add(1)
            .ok_or_else(|| ProtocolError::Violation(AEAD_ERR_OVERFLOW.into()))?;

        Ok(plaintext)
    }

    /// Returns the expected sequence number of the next incoming frame.
    pub fn expected_seq(&self) -> u64 {
        self.expected_seq
    }
}

/// Derives directional AEAD keys from the HMAC session key.
pub fn derive_aead_keys(
    session_key: &[u8; 32],
    role: EndpointRole,
) -> (AeadWriterSession, AeadReaderSession) {
    let key = hmac::Key::new(hmac::HMAC_SHA256, session_key);
    let c2s = hmac::sign(&key, b"oxs1-aead-c2s");
    let s2c = hmac::sign(&key, b"oxs1-aead-s2c");

    let mut client_write_key = [0u8; 32];
    client_write_key.copy_from_slice(c2s.as_ref());

    let mut server_write_key = [0u8; 32];
    server_write_key.copy_from_slice(s2c.as_ref());

    match role {
        EndpointRole::Client => (
            AeadWriterSession::new(&client_write_key),
            AeadReaderSession::new(&server_write_key),
        ),
        EndpointRole::Server => (
            AeadWriterSession::new(&server_write_key),
            AeadReaderSession::new(&client_write_key),
        ),
    }
}

/// Constructs a `LengthDelimitedCodec` configured for rusty_grid:
/// - 4-byte length prefix
/// - Big-endian byte order
/// - Strips length prefix on decode
/// - Rejects frames > 64 MB
pub fn default_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(LENGTH_FIELD_BYTES)
        .length_adjustment(0)
        .num_skip(LENGTH_FIELD_BYTES)
        .big_endian()
        .max_frame_length(MAX_FRAME_SIZE)
        .new_codec()
}

/// Serializes a message into a byte frame according to the specified wire codec.
pub fn serialize_message<M: Serialize>(msg: &M, codec: WireCodec) -> Result<Bytes, ProtocolError> {
    match codec {
        WireCodec::Bincode => {
            let payload_len = bincode::serialized_size(msg)? as usize;
            let size = 1usize.saturating_add(payload_len);
            if size > MAX_FRAME_SIZE {
                return Err(ProtocolError::FrameTooLarge {
                    size,
                    max: MAX_FRAME_SIZE,
                });
            }
            let mut buf = Vec::with_capacity(size);
            buf.push(WIRE_FORMAT_BINCODE);
            bincode::serialize_into(&mut buf, msg)?;
            Ok(Bytes::from(buf))
        }
        WireCodec::Json => {
            let mut buf = Vec::with_capacity(128);
            buf.push(WIRE_FORMAT_JSON);
            serde_json::to_writer(&mut buf, msg)?;
            if buf.len() > MAX_FRAME_SIZE {
                return Err(ProtocolError::FrameTooLarge {
                    size: buf.len(),
                    max: MAX_FRAME_SIZE,
                });
            }
            Ok(Bytes::from(buf))
        }
    }
}

/// Serializes a message for a peer speaking `peer_version` (design 0083 D3).
///
/// For the current version this is [`serialize_message`]. For an older one the messages whose
/// layout or stamp differ are re-encoded: `MasterMessage` (the accepted `RegisterAck` echoes the
/// peer's version, tasks go through `Task::for_peer` and, in bincode, take the v9 task layout)
/// and `ClientResponse` (same layout, stamp = the peer's version, except `Error`, which keeps
/// stamping the current version that a peer reads without a check). Every other message has the
/// same bytes in both versions.
pub fn serialize_message_for_peer<M: Serialize + 'static>(
    msg: &M,
    codec: WireCodec,
    peer_version: u32,
) -> Result<Bytes, ProtocolError> {
    if peer_version >= PROTOCOL_VERSION_CURRENT {
        return serialize_message(msg, codec);
    }
    let any: &dyn std::any::Any = msg;
    if let Some(master) = any.downcast_ref::<MasterMessage>() {
        let adapted = master_message_for_peer(master, peer_version);
        return match codec {
            WireCodec::Json => serialize_message(&adapted, WireCodec::Json),
            WireCodec::Bincode => {
                let payload = serialize_master_message_v9_bincode(adapted)?;
                let size = 1usize.saturating_add(payload.len());
                if size > MAX_FRAME_SIZE {
                    return Err(ProtocolError::FrameTooLarge {
                        size,
                        max: MAX_FRAME_SIZE,
                    });
                }
                let mut buf = Vec::with_capacity(size);
                buf.push(WIRE_FORMAT_BINCODE);
                buf.extend_from_slice(&payload);
                Ok(Bytes::from(buf))
            }
        };
    }
    if let Some(resp) = any.downcast_ref::<ClientResponse>() {
        if matches!(resp, ClientResponse::Error { .. }) {
            return serialize_message(resp, codec);
        }
        return serialize_client_response_with_version(resp, peer_version, codec);
    }
    serialize_message(msg, codec)
}

/// Serializes a [`ClientMessage`] as a v9 client sends it (design 0083, for tests): stamped 9, and
/// a `SubmitTask` in the v9 task layout without extensions. Every other message has the same
/// layout in both versions. The layout is the one `wire_layout_previous.txt` (v0.3.0) pins.
#[cfg(any(test, feature = "test-extensions"))]
pub fn serialize_v9_client_message(
    msg: &ClientMessage,
    codec: WireCodec,
) -> Result<Bytes, ProtocolError> {
    use super::client_msg_mirrors::LegacyV9Task;
    const V9: u32 = 9;
    if let (ClientMessage::SubmitTask { task, wait }, WireCodec::Bincode) = (msg, codec) {
        return serialize_message(
            &LegacyV9BinaryClientMessage::SubmitTask {
                task: LegacyV9Task::from(task.for_peer(V9)),
                wait: *wait,
                protocol_version: V9,
            },
            WireCodec::Bincode,
        );
    }
    let msg = match msg {
        ClientMessage::SubmitTask { task, wait } => ClientMessage::SubmitTask {
            task: task.for_peer(V9),
            wait: *wait,
        },
        other => other.clone(),
    };
    match codec {
        WireCodec::Bincode => serialize_message(
            &BinaryClientMessage::from_client_message(msg, V9),
            WireCodec::Bincode,
        ),
        WireCodec::Json => serialize_message(
            &HumanClientMessage::from_client_message(msg, V9),
            WireCodec::Json,
        ),
    }
}

/// Reads a frame the way a v9 peer does (a frozen reader for tests, design 0083): a
/// `MasterMessage` in the v9 task layout whose accepted `RegisterAck` must carry version 9, a
/// `ClientResponse` whose stamp must be 9 unless it is an `Error`. Any other type has the same
/// layout in both versions and goes through [`deserialize_message`].
#[cfg(any(test, feature = "test-extensions"))]
pub fn deserialize_message_as_v9<M: DeserializeOwned + 'static>(
    bytes: &[u8],
) -> Result<(M, WireCodec), ProtocolError> {
    const V9: u32 = 9;
    let tid = std::any::TypeId::of::<M>();
    if tid == std::any::TypeId::of::<MasterMessage>() {
        let (msg, codec) = match bytes.first().copied() {
            Some(WIRE_FORMAT_BINCODE) => (
                super::worker_master_mirrors::deserialize_master_message_v9_bincode(&bytes[1..])?,
                WireCodec::Bincode,
            ),
            _ => {
                let (m, c) = deserialize_message::<MasterMessage>(bytes)?;
                (m, c)
            }
        };
        if let MasterMessage::RegisterAck {
            accepted: true,
            protocol_version,
            ..
        } = &msg
        {
            if *protocol_version != V9 {
                return Err(ProtocolError::IncompatibleVersion {
                    peer_version: *protocol_version,
                    current_version: V9,
                    min_compatible_version: V9,
                });
            }
        }
        return Ok((downcast_msg(msg), codec));
    }
    if tid == std::any::TypeId::of::<ClientResponse>() {
        let (stamp, is_error, msg, codec) = match bytes.first().copied() {
            Some(WIRE_FORMAT_BINCODE) => {
                let bin: BinaryClientResponse =
                    bincode_exact_deserialize(&bytes[1..]).map_err(ProtocolError::Bincode)?;
                (
                    bin.protocol_version(),
                    bin.is_error(),
                    ClientResponse::from(bin),
                    WireCodec::Bincode,
                )
            }
            Some(WIRE_FORMAT_JSON) => {
                let human: HumanClientResponse = serde_json::from_slice(&bytes[1..])?;
                (
                    human.protocol_version(),
                    human.is_error(),
                    ClientResponse::from(human),
                    WireCodec::Json,
                )
            }
            Some(tag) => return Err(ProtocolError::InvalidFormatTag(tag)),
            None => return Err(ProtocolError::UnexpectedEof),
        };
        if !is_error && stamp != V9 {
            return Err(ProtocolError::IncompatibleVersion {
                peer_version: stamp,
                current_version: V9,
                min_compatible_version: V9,
            });
        }
        return Ok((downcast_msg(msg), codec));
    }
    deserialize_message::<M>(bytes)
}

pub(super) fn bincode_exact_deserialize<T: DeserializeOwned>(
    slice: &[u8],
) -> Result<T, Box<bincode::ErrorKind>> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(slice.len() as u64)
        .deserialize(slice)
}

fn downcast_msg<T: 'static, M: 'static>(val: T) -> M {
    let boxed: Box<dyn std::any::Any> = Box::new(val);
    *boxed
        .downcast::<M>()
        .expect("caller checked TypeId::of::<M>() == TypeId::of::<T>()")
}

fn incompatible_version_error(peer_version: u32) -> ProtocolError {
    ProtocolError::IncompatibleVersion {
        peer_version,
        current_version: PROTOCOL_VERSION_CURRENT,
        min_compatible_version: PROTOCOL_VERSION_MIN_COMPATIBLE,
    }
}

/// The protocol version whose `Task` layout (no `extensions`) a v10 Master still reads in a
/// `SubmitTask` (design 0083 D4).
const WINDOW_LEGACY_TASK_VERSION: u32 = 9;

/// Decodes `slice` as the v9 `SubmitTask` its stamp `stamp` claims, and as nothing else: a frame
/// is never accepted under a layout its stamp contradicts (design 0083 D2).
fn decode_legacy_v9_submit(slice: &[u8], stamp: u32) -> Result<ClientMessage, ProtocolError> {
    match bincode_exact_deserialize::<LegacyV9BinaryClientMessage>(slice) {
        Ok(LegacyV9BinaryClientMessage::SubmitTask {
            task,
            wait,
            protocol_version,
        }) if protocol_version == stamp && stamp == WINDOW_LEGACY_TASK_VERSION => {
            Ok(ClientMessage::SubmitTask {
                task: task.into(),
                wait,
            })
        }
        Ok(_) => Err(ProtocolError::Bincode(Box::new(
            bincode::ErrorKind::Custom(format!(
                "SubmitTask stamped {stamp} does not match the layout of that version"
            )),
        ))),
        Err(e) => Err(ProtocolError::Bincode(e)),
    }
}

/// Decodes a bincode [`ClientMessage`] and returns it with its protocol version stamp.
fn decode_bincode_client_message(slice: &[u8]) -> Result<(ClientMessage, u32), ProtocolError> {
    match bincode_exact_deserialize::<BinaryClientMessage>(slice) {
        Ok(bin_msg) => {
            let ver = bin_msg.protocol_version();
            if !is_protocol_version_compatible(ver) {
                return Err(incompatible_version_error(ver));
            }
            // A v9 `SubmitTask` can parse as a v10 one by accident (design 0083 D2: a
            // `created_at_utc` of 0 reads as an empty extension list). When the stamp is below the
            // current version, the layout of that stamp decides.
            if ver < PROTOCOL_VERSION_CURRENT
                && matches!(bin_msg, BinaryClientMessage::SubmitTask { .. })
            {
                return decode_legacy_v9_submit(slice, ver).map(|m| (m, ver));
            }
            Ok((bin_msg.into(), ver))
        }
        Err(err) => {
            if bincode_exact_deserialize::<LegacyV2BinaryClientMessage>(slice).is_ok() {
                return Err(incompatible_version_error(2));
            }
            if let Ok(LegacyV5BinaryClientMessage::SubmitTask {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV5BinaryClientMessage>(slice)
            {
                if !is_protocol_version_compatible(protocol_version) {
                    return Err(incompatible_version_error(protocol_version));
                }
            }
            if let Ok(LegacyV6ToV8BinaryClientMessage::SubmitTask {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV6ToV8BinaryClientMessage>(slice)
            {
                if !is_protocol_version_compatible(protocol_version) {
                    return Err(incompatible_version_error(protocol_version));
                }
            }
            if let Ok(LegacyV9BinaryClientMessage::SubmitTask {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV9BinaryClientMessage>(slice)
            {
                if !is_protocol_version_compatible(protocol_version) {
                    return Err(incompatible_version_error(protocol_version));
                }
                if protocol_version == WINDOW_LEGACY_TASK_VERSION {
                    return decode_legacy_v9_submit(slice, protocol_version)
                        .map(|m| (m, protocol_version));
                }
            }
            Err(ProtocolError::Bincode(err))
        }
    }
}

fn decode_bincode_client_response(slice: &[u8]) -> Result<ClientResponse, ProtocolError> {
    match bincode_exact_deserialize::<BinaryClientResponse>(slice) {
        Ok(bin_resp) => {
            let ver = bin_resp.protocol_version();
            if !bin_resp.is_error() && !is_master_echo_acceptable(ver) {
                return Err(incompatible_version_error(ver));
            }
            Ok(bin_resp.into())
        }
        Err(err) => {
            if let Ok(LegacyV3V4BinaryClientResponse::ClusterStatus {
                protocol_version, ..
            }) = bincode_exact_deserialize::<LegacyV3V4BinaryClientResponse>(slice)
            {
                return Err(incompatible_version_error(protocol_version));
            }
            if let Ok(legacy_resp) =
                bincode_exact_deserialize::<LegacyV2BinaryClientResponse>(slice)
            {
                if let LegacyV2BinaryClientResponse::Error { message } = legacy_resp {
                    return Ok(ClientResponse::Error { message });
                }
                return Err(incompatible_version_error(2));
            }
            Err(ProtocolError::Bincode(err))
        }
    }
}

fn decode_bincode_inbound_message(
    slice: &[u8],
) -> Result<(InboundMessage, Option<u32>), ProtocolError> {
    // 1. Bare modern WorkerMessage opening with Register.
    if let Ok(w) = bincode_exact_deserialize::<WorkerMessage>(slice) {
        if matches!(w, WorkerMessage::Register { .. }) {
            return Ok((InboundMessage::Worker(w), None));
        }
    }

    // 2. Bare ClientMessage (checks protocol_version and legacy v2 client layout).
    let client_res = decode_bincode_client_message(slice);
    if let Ok((c, ver)) = &client_res {
        return Ok((InboundMessage::Client(c.clone()), Some(*ver)));
    }

    // 3. Bare non-Register WorkerMessage.
    if let Ok(w) = bincode_exact_deserialize::<WorkerMessage>(slice) {
        return Ok((InboundMessage::Worker(w), None));
    }

    // 4. Bare legacy WorkerMessage.
    if let Ok(legacy) = bincode_exact_deserialize::<LegacyBinaryWorkerMessage>(slice) {
        return Ok((InboundMessage::Worker(legacy.into()), None));
    }

    // 5. Wrapped BinaryInboundMessage.
    if let Ok(wrapped) = bincode_exact_deserialize::<BinaryInboundMessage>(slice) {
        return Ok((wrapped.into(), None));
    }

    // 6. Surface version mismatch when the frame matched a ClientMessage layout with an
    // incompatible protocol version.
    if let Err(err @ ProtocolError::IncompatibleVersion { .. }) = client_res {
        return Err(err);
    }

    // 7. Unknown future WorkerMessage variant.
    if slice.len() >= 4 {
        let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
        if (8..10_000).contains(&tag) {
            return Ok((
                InboundMessage::Worker(WorkerMessage::UnknownVariant {
                    tag,
                    raw_bytes: slice[4..].to_vec(),
                }),
                None,
            ));
        }
    }

    Err(client_res.unwrap_err())
}

fn decode_json_client_message(raw: &[u8]) -> Result<(ClientMessage, u32), ProtocolError> {
    let human: HumanClientMessage = serde_json::from_slice(raw)?;
    let ver = human.protocol_version();
    if !is_protocol_version_compatible(ver) {
        return Err(incompatible_version_error(ver));
    }
    Ok((human.into(), ver))
}

fn decode_json_client_response(raw: &[u8]) -> Result<ClientResponse, ProtocolError> {
    let human: HumanClientResponse = serde_json::from_slice(raw)?;
    let ver = human.protocol_version();
    if !human.is_error() && !is_master_echo_acceptable(ver) {
        return Err(incompatible_version_error(ver));
    }
    Ok(human.into())
}

fn decode_json_typed<M: DeserializeOwned + 'static>(
    raw: &[u8],
) -> Result<(M, Option<u32>), ProtocolError> {
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientMessage>() {
        return decode_json_client_message(raw).map(|(m, v)| (downcast_msg(m), Some(v)));
    }
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientResponse>() {
        return decode_json_client_response(raw).map(|m| (downcast_msg(m), None));
    }
    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
        if let Ok(w) = serde_json::from_slice::<WorkerMessage>(raw) {
            return Ok((downcast_msg(InboundMessage::Worker(w)), None));
        }
        return decode_json_client_message(raw)
            .map(|(c, v)| (downcast_msg(InboundMessage::Client(c)), Some(v)));
    }
    serde_json::from_slice::<M>(raw)
        .map(|m| (m, None))
        .map_err(ProtocolError::Json)
}

fn peek_wire_codec(bytes: &[u8]) -> Option<WireCodec> {
    match bytes.first().copied() {
        Some(WIRE_FORMAT_BINCODE) => Some(WireCodec::Bincode),
        Some(WIRE_FORMAT_JSON | b'{' | b' ' | b'\t' | b'\r' | b'\n') => Some(WireCodec::Json),
        _ => None,
    }
}

/// Deserializes a message from a byte frame, automatically identifying the wire format
/// from the 1-byte discriminator tag or legacy raw JSON format.
pub fn deserialize_message<M: DeserializeOwned + 'static>(
    bytes: &[u8],
) -> Result<(M, WireCodec), ProtocolError> {
    deserialize_message_with_version(bytes).map(|(msg, codec, _)| (msg, codec))
}

/// Like [`deserialize_message`], and also returns the protocol version stamped on a
/// [`ClientMessage`] (or on the one inside an [`InboundMessage`]): the version of the peer that
/// sent it (design 0083 D2). It is `None` for every other message.
pub fn deserialize_message_with_version<M: DeserializeOwned + 'static>(
    bytes: &[u8],
) -> Result<(M, WireCodec, Option<u32>), ProtocolError> {
    if bytes.is_empty() {
        return serde_json::from_slice::<M>(bytes)
            .map(|m| (m, WireCodec::Json, None))
            .map_err(ProtocolError::Json);
    }

    match bytes[0] {
        WIRE_FORMAT_BINCODE => {
            let slice = &bytes[1..];
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientMessage>() {
                return decode_bincode_client_message(slice)
                    .map(|(msg, ver)| (downcast_msg(msg), WireCodec::Bincode, Some(ver)));
            }
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<ClientResponse>() {
                return decode_bincode_client_response(slice)
                    .map(|msg| (downcast_msg(msg), WireCodec::Bincode, None));
            }
            if std::any::TypeId::of::<M>() == std::any::TypeId::of::<InboundMessage>() {
                return decode_bincode_inbound_message(slice)
                    .map(|(msg, ver)| (downcast_msg(msg), WireCodec::Bincode, ver));
            }

            match bincode_exact_deserialize::<M>(slice) {
                Ok(msg) => Ok((msg, WireCodec::Bincode, None)),
                Err(e) => {
                    // Fallback for WorkerMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<WorkerMessage>() {
                        if let Ok(legacy) =
                            bincode_exact_deserialize::<LegacyBinaryWorkerMessage>(slice)
                        {
                            let msg: WorkerMessage = legacy.into();
                            return Ok((downcast_msg(msg), WireCodec::Bincode, None));
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (8..10_000).contains(&tag) {
                                let unknown = WorkerMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                return Ok((downcast_msg(unknown), WireCodec::Bincode, None));
                            }
                        }
                    }

                    // Fallback for MasterMessage (legacy or unknown variant)
                    if std::any::TypeId::of::<M>() == std::any::TypeId::of::<MasterMessage>() {
                        if let Ok(legacy) =
                            bincode_exact_deserialize::<LegacyBinaryMasterMessage>(slice)
                        {
                            let msg: MasterMessage = legacy.into();
                            return Ok((downcast_msg(msg), WireCodec::Bincode, None));
                        }
                        if slice.len() >= 4 {
                            let tag = u32::from_le_bytes(slice[0..4].try_into().unwrap());
                            if (7..10_000).contains(&tag) {
                                let unknown = MasterMessage::UnknownVariant {
                                    tag,
                                    raw_bytes: slice[4..].to_vec(),
                                };
                                return Ok((downcast_msg(unknown), WireCodec::Bincode, None));
                            }
                        }
                    }

                    Err(ProtocolError::Bincode(e))
                }
            }
        }
        WIRE_FORMAT_JSON => {
            decode_json_typed::<M>(&bytes[1..]).map(|(msg, ver)| (msg, WireCodec::Json, ver))
        }
        b'{' | b' ' | b'\t' | b'\r' | b'\n' => {
            decode_json_typed::<M>(bytes).map(|(msg, ver)| (msg, WireCodec::Json, ver))
        }
        tag => Err(ProtocolError::InvalidFormatTag(tag)),
    }
}

/// Unified bidirectional framed transport over any `AsyncRead + AsyncWrite + Unpin` stream.
pub struct MessageTransport<T> {
    inner: Framed<T, LengthDelimitedCodec>,
    codec: WireCodec,
    /// Protocol version the peer speaks, for outbound messages (design 0083 D3).
    peer_version: u32,
    last_detected_codec: Option<WireCodec>,
    /// Protocol version stamped on the most recently received client message.
    last_peer_version: Option<u32>,
    aead_writer: Option<AeadWriterSession>,
    aead_reader: Option<AeadReaderSession>,
}

impl<T> MessageTransport<T> {
    /// Wraps an I/O stream with the standard 4-byte length-delimited framing codec
    /// using the default wire codec (Bincode).
    pub fn new(io: T) -> Self {
        Self::with_codec(io, WireCodec::default())
    }

    /// Wraps an I/O stream with the standard 4-byte length-delimited framing codec
    /// using a specific outbound wire codec.
    pub fn with_codec(io: T, codec: WireCodec) -> Self {
        Self {
            inner: Framed::new(io, default_codec()),
            codec,
            peer_version: PROTOCOL_VERSION_CURRENT,
            last_detected_codec: None,
            last_peer_version: None,
            aead_writer: None,
            aead_reader: None,
        }
    }

    /// Enables AEAD frame encryption/decryption using directional keys derived from `session_key`.
    pub fn enable_encryption(&mut self, session_key: &[u8; 32], role: EndpointRole) {
        let (writer, reader) = derive_aead_keys(session_key, role);
        self.aead_writer = Some(writer);
        self.aead_reader = Some(reader);
    }

    /// Returns `true` if this transport has AEAD encryption enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_writer.is_some() || self.aead_reader.is_some()
    }

    /// Returns the currently configured outbound wire codec.
    pub fn codec(&self) -> WireCodec {
        self.codec
    }

    /// Sets the outbound wire codec.
    pub fn set_outbound_codec(&mut self, codec: WireCodec) {
        self.codec = codec;
    }

    /// Returns the wire codec detected from the most recently received frame, if any.
    pub fn last_detected_codec(&self) -> Option<WireCodec> {
        self.last_detected_codec
    }

    /// Returns the protocol version outbound messages are encoded for.
    pub fn peer_version(&self) -> u32 {
        self.peer_version
    }

    /// Sets the protocol version outbound messages are encoded for (design 0083 D3). The default
    /// is [`PROTOCOL_VERSION_CURRENT`], for which nothing changes.
    pub fn set_peer_version(&mut self, version: u32) {
        self.peer_version = version;
    }

    /// Returns the protocol version stamped on the most recently received client message, if any
    /// (design 0083 D2).
    pub fn last_peer_version(&self) -> Option<u32> {
        self.last_peer_version
    }

    /// Records the version of a client message that was decoded outside this transport (from a
    /// raw frame).
    pub fn note_peer_version(&mut self, version: u32) {
        self.last_peer_version = Some(version);
    }

    /// Consumes this transport, returning the underlying `Framed` stream.
    pub fn into_inner(self) -> Framed<T, LengthDelimitedCodec> {
        self.inner
    }

    /// Returns a reference to the underlying I/O stream.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    /// Returns a mutable reference to the underlying I/O stream.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    /// Returns the number of unread bytes currently buffered in the framing reader.
    pub fn read_buffer_len(&self) -> usize {
        self.inner.read_buffer().len()
    }

    /// Returns the number of unwritten bytes currently buffered in the framing writer.
    pub fn write_buffer_len(&self) -> usize {
        self.inner.write_buffer().len()
    }
}

impl MessageTransport<crate::transport::GridStream> {
    /// Gracefully closes the underlying client P2P endpoint (if this transport wraps a
    /// [`crate::transport::GridStream::P2p`]). No-op for TCP and in-memory streams.
    pub async fn close(&mut self) {
        self.get_mut().close().await;
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> MessageTransport<T> {
    /// Splits transport into independent read and write halves via `tokio::io::split`,
    /// preserving any unconsumed read buffer or unflushed write buffer, propagating
    /// the configured outbound wire codec and AEAD crypto sessions.
    pub fn split(self) -> (MessageWriter<WriteHalf<T>>, MessageReader<ReadHalf<T>>) {
        let parts = self.inner.into_parts();
        let (read_half, write_half) = split(parts.io);
        let mut writer = MessageWriter::with_codec_buffer_and_aead(
            write_half,
            self.codec,
            parts.write_buf,
            self.aead_writer,
        );
        writer.set_peer_version(self.peer_version);
        let mut reader =
            MessageReader::with_buffer_and_aead(read_half, parts.read_buf, self.aead_reader);
        reader.last_peer_version = self.last_peer_version;
        (writer, reader)
    }

    /// Serializes and sends a message framed with a 4-byte length prefix using the configured wire codec.
    /// If encryption is enabled, the frame is encrypted using the AEAD writer session.
    pub async fn send_msg<M: Serialize + 'static>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let payload = serialize_message_for_peer(msg, self.codec, self.peer_version)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        self.inner.send(frame).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Serializes and sends a message framed with a 4-byte length prefix, requiring write progress
    /// at least once every `idle_timeout` so large frames over slow links succeed as long as bytes
    /// continue to drain.
    pub async fn send_msg_with_idle_timeout<M: Serialize + 'static>(
        &mut self,
        msg: &M,
        idle_timeout: std::time::Duration,
    ) -> Result<(), ProtocolError> {
        let payload = serialize_message_for_peer(msg, self.codec, self.peer_version)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        tokio::time::timeout(idle_timeout, self.inner.feed(frame))
            .await
            .map_err(|_| {
                ProtocolError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "write timed out",
                ))
            })?
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    ProtocolError::UnexpectedEof
                } else {
                    ProtocolError::Io(e)
                }
            })?;
        let mut last_remaining = self.inner.write_buffer().len();
        loop {
            match tokio::time::timeout(idle_timeout, SinkExt::<Bytes>::flush(&mut self.inner)).await
            {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => {
                    return if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        Err(ProtocolError::UnexpectedEof)
                    } else {
                        Err(ProtocolError::Io(e))
                    };
                }
                Err(_) => {
                    let now_remaining = self.inner.write_buffer().len();
                    if now_remaining < last_remaining {
                        last_remaining = now_remaining;
                        continue;
                    }
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "write timed out",
                    )));
                }
            }
        }
    }

    /// Reads the next framed message from the stream.
    /// Returns `Ok(Some(msg))` on success, `Ok(None)` on clean EOF, or `Err`.
    pub async fn recv_msg<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<M>, ProtocolError> {
        match self.recv_msg_with_codec::<M>().await? {
            Some((msg, _codec)) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    /// Reads the next framed message from the stream, requiring read progress at least once every
    /// `idle_timeout` so large frames (up to 64 MB) over slow links succeed as long as bytes
    /// continue arriving.
    pub async fn recv_msg_with_idle_timeout<M: DeserializeOwned + 'static>(
        &mut self,
        idle_timeout: std::time::Duration,
    ) -> Result<Option<M>, ProtocolError> {
        let mut last_state = (
            self.inner.read_buffer().len(),
            self.inner.read_buffer().capacity(),
        );
        loop {
            match tokio::time::timeout(idle_timeout, self.recv_msg::<M>()).await {
                Ok(res) => return res,
                Err(_) => {
                    let now_state = (
                        self.inner.read_buffer().len(),
                        self.inner.read_buffer().capacity(),
                    );
                    if now_state.0 > last_state.0 || now_state.1 > last_state.1 {
                        last_state = now_state;
                        continue;
                    }
                    return Err(ProtocolError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "read timed out",
                    )));
                }
            }
        }
    }

    /// Reads the next framed message and returns it alongside the detected wire codec.
    /// If encryption is enabled, the frame is opened and verified using the AEAD reader session.
    pub async fn recv_msg_with_codec<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<(M, WireCodec)>, ProtocolError> {
        self.recv_msg_with_codec_max(MAX_FRAME_SIZE).await
    }

    /// Reads the next framed message with a temporary frame-length cap `max_len`, restoring
    /// [`MAX_FRAME_SIZE`] only after the frame is read and (when encryption is active) decrypted.
    pub async fn recv_msg_with_codec_max<M: DeserializeOwned + 'static>(
        &mut self,
        max_len: usize,
    ) -> Result<Option<(M, WireCodec)>, ProtocolError> {
        let limit = max_len.min(MAX_FRAME_SIZE);
        self.inner.codec_mut().set_max_frame_length(limit);
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let plaintext = match self.aead_reader.as_mut() {
                    Some(r) => r.decrypt_frame(&bytes)?,
                    None => bytes.freeze(),
                };
                self.inner.codec_mut().set_max_frame_length(MAX_FRAME_SIZE);
                if let Some(detected) = peek_wire_codec(&plaintext) {
                    self.last_detected_codec = Some(detected);
                }
                let (msg, codec, version) = deserialize_message_with_version::<M>(&plaintext)?;
                self.last_detected_codec = Some(codec);
                if version.is_some() {
                    self.last_peer_version = version;
                }
                Ok(Some((msg, codec)))
            }
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: limit,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Returns the current capacity of the underlying frame reader buffer.
    pub fn read_buffer_capacity(&self) -> usize {
        self.inner.read_buffer().capacity()
    }

    /// Sends raw bytes as a framed message.
    pub async fn send_raw_frame(&mut self, data: Bytes) -> Result<(), ProtocolError> {
        if data.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: data.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner.send(data).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Reads raw frame bytes without deserializing.
    pub async fn recv_raw_frame(&mut self) -> Result<Option<Bytes>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Reads raw frame bytes without deserializing, refusing frames longer than `max_len`.
    pub async fn recv_raw_frame_max(
        &mut self,
        max_len: usize,
    ) -> Result<Option<Bytes>, ProtocolError> {
        let limit = max_len.min(MAX_FRAME_SIZE);
        self.inner.codec_mut().set_max_frame_length(limit);
        let res = match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: limit,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        };
        self.inner.codec_mut().set_max_frame_length(MAX_FRAME_SIZE);
        res
    }
}

/// Unidirectional framed reader for receiving messages.
pub struct MessageReader<R> {
    inner: Framed<R, LengthDelimitedCodec>,
    last_detected_codec: Option<WireCodec>,
    last_peer_version: Option<u32>,
    aead_reader: Option<AeadReaderSession>,
}

impl<R: AsyncRead + Unpin> MessageReader<R> {
    /// Creates a new `MessageReader` wrapping an async reader.
    pub fn new(reader: R) -> Self {
        Self::with_buffer(reader, BytesMut::new())
    }

    /// Creates a new `MessageReader` wrapping an async reader with pre-buffered data.
    pub fn with_buffer(reader: R, buffer: BytesMut) -> Self {
        Self::with_buffer_and_aead(reader, buffer, None)
    }

    /// Creates a new `MessageReader` with pre-buffered data and optional AEAD reader session.
    pub fn with_buffer_and_aead(
        reader: R,
        buffer: BytesMut,
        aead_reader: Option<AeadReaderSession>,
    ) -> Self {
        let mut parts = FramedParts::new::<Bytes>(reader, default_codec());
        parts.read_buf = buffer;
        Self {
            inner: Framed::from_parts(parts),
            last_detected_codec: None,
            last_peer_version: None,
            aead_reader,
        }
    }

    /// Enables AEAD decryption session.
    pub fn enable_encryption(&mut self, reader: AeadReaderSession) {
        self.aead_reader = Some(reader);
    }

    /// Returns `true` if AEAD decryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_reader.is_some()
    }

    /// Reads the next framed message from the reader.
    pub async fn recv_msg<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<M>, ProtocolError> {
        match self.recv_msg_with_codec::<M>().await? {
            Some((msg, _codec)) => Ok(Some(msg)),
            None => Ok(None),
        }
    }

    /// Reads the next framed message and returns it alongside the detected wire codec.
    pub async fn recv_msg_with_codec<M: DeserializeOwned + 'static>(
        &mut self,
    ) -> Result<Option<(M, WireCodec)>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => {
                let plaintext = match self.aead_reader.as_mut() {
                    Some(r) => r.decrypt_frame(&bytes)?,
                    None => bytes.freeze(),
                };
                if let Some(detected) = peek_wire_codec(&plaintext) {
                    self.last_detected_codec = Some(detected);
                }
                let (msg, codec, version) = deserialize_message_with_version::<M>(&plaintext)?;
                self.last_detected_codec = Some(codec);
                if version.is_some() {
                    self.last_peer_version = version;
                }
                Ok(Some((msg, codec)))
            }
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }

    /// Returns the wire codec detected from the most recently received frame, if any.
    pub fn last_detected_codec(&self) -> Option<WireCodec> {
        self.last_detected_codec
    }

    /// Returns the protocol version stamped on the most recently received client message, if any
    /// (design 0083 D2).
    pub fn last_peer_version(&self) -> Option<u32> {
        self.last_peer_version
    }

    /// Reads raw frame bytes without deserializing.
    pub async fn recv_raw_frame(&mut self) -> Result<Option<Bytes>, ProtocolError> {
        match self.inner.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Err(ProtocolError::UnexpectedEof)
                } else if e.kind() == std::io::ErrorKind::InvalidData {
                    Err(ProtocolError::FrameTooLarge {
                        size: 0,
                        max: MAX_FRAME_SIZE,
                    })
                } else {
                    Err(ProtocolError::Io(e))
                }
            }
            None => Ok(None),
        }
    }
}

/// Unidirectional framed writer for transmitting messages.
pub struct MessageWriter<W> {
    inner: FramedWrite<W, LengthDelimitedCodec>,
    codec: WireCodec,
    /// Protocol version the peer speaks, for outbound messages (design 0083 D3).
    peer_version: u32,
    aead_writer: Option<AeadWriterSession>,
}

impl<W: AsyncWrite + Unpin> MessageWriter<W> {
    /// Creates a new `MessageWriter` wrapping an async writer with default wire codec (Bincode).
    pub fn new(writer: W) -> Self {
        Self::with_codec(writer, WireCodec::default())
    }

    /// Creates a new `MessageWriter` wrapping an async writer with specific wire codec.
    pub fn with_codec(writer: W, codec: WireCodec) -> Self {
        Self::with_codec_buffer_and_aead(writer, codec, BytesMut::new(), None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and default codec.
    pub fn with_buffer(writer: W, buffer: BytesMut) -> Self {
        Self::with_codec_buffer_and_aead(writer, WireCodec::default(), buffer, None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data and specified codec.
    pub fn with_codec_and_buffer(writer: W, codec: WireCodec, buffer: BytesMut) -> Self {
        Self::with_codec_buffer_and_aead(writer, codec, buffer, None)
    }

    /// Creates a new `MessageWriter` wrapping an async writer with pre-buffered data, specified codec, and AEAD session.
    pub fn with_codec_buffer_and_aead(
        writer: W,
        codec: WireCodec,
        buffer: BytesMut,
        aead_writer: Option<AeadWriterSession>,
    ) -> Self {
        let mut framed = FramedWrite::new(writer, default_codec());
        if !buffer.is_empty() {
            framed.write_buffer_mut().extend_from_slice(&buffer);
        }
        Self {
            inner: framed,
            codec,
            peer_version: PROTOCOL_VERSION_CURRENT,
            aead_writer,
        }
    }

    /// Enables AEAD encryption session.
    pub fn enable_encryption(&mut self, writer: AeadWriterSession) {
        self.aead_writer = Some(writer);
    }

    /// Returns `true` if AEAD encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.aead_writer.is_some()
    }

    /// Returns the currently configured outbound wire codec.
    pub fn codec(&self) -> WireCodec {
        self.codec
    }

    /// Sets the outbound wire codec.
    pub fn set_codec(&mut self, codec: WireCodec) {
        self.codec = codec;
    }

    /// Returns the protocol version outbound messages are encoded for.
    pub fn peer_version(&self) -> u32 {
        self.peer_version
    }

    /// Sets the protocol version outbound messages are encoded for (design 0083 D3).
    pub fn set_peer_version(&mut self, version: u32) {
        self.peer_version = version;
    }

    /// Serializes and sends a message framed with a 4-byte length prefix using the configured wire codec.
    pub async fn send_msg<M: Serialize + 'static>(&mut self, msg: &M) -> Result<(), ProtocolError> {
        let payload = serialize_message_for_peer(msg, self.codec, self.peer_version)?;
        let frame = match self.aead_writer.as_mut() {
            Some(w) => w.encrypt_frame(&payload)?,
            None => payload,
        };
        self.inner.send(frame).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }

    /// Sends raw bytes as a framed message.
    pub async fn send_raw_frame(&mut self, data: Bytes) -> Result<(), ProtocolError> {
        if data.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge {
                size: data.len(),
                max: MAX_FRAME_SIZE,
            });
        }
        self.inner.send(data).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ProtocolError::UnexpectedEof
            } else {
                ProtocolError::Io(e)
            }
        })?;
        Ok(())
    }
}
