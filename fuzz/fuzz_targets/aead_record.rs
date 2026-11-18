#![no_main]
//! LV-18 target 4: the encrypted record layer (format byte `0x04`) that carries every frame after
//! the handshake: `AeadReaderSession::decrypt_frame`, and the readers that call it
//! (`MessageTransport` and the `MessageReader` of `split`) with encryption enabled. The record
//! keys are derived from a fixed test session key, as after a completed handshake.
//!
//! Input: the first byte picks the mode.
//! - even: the rest is the byte stream of a peer that does not hold the key, read by the server
//!   side as `InboundMessage` with a frame limit of 4096 bytes and at most 64 frames.
//! - odd: the rest is a list of chunks `[len: u16 LE][op][plaintext]` (see `chunks`). The client
//!   side seals each plaintext; `op` then keeps, damages, drops, replays or cuts the record. The
//!   server's reader must accept exactly the records that arrive intact and in order before the
//!   first bad one, return each plaintext unchanged, and refuse every record after the first bad
//!   one (it fails closed). The same records then go through the `MessageReader` of a split
//!   transport, which decodes the plaintexts as `InboundMessage`; it may only return a message
//!   for a record the first reader accepted.

use libfuzzer_sys::fuzz_target;
use rusty_grid_core::protocol::{
    derive_aead_keys, EndpointRole, InboundMessage, MessageTransport, WireCodec,
};
use rusty_grid_fuzz::{block_on, chunks, frame, FuzzIo, TEST_SESSION_KEY};

/// Small on purpose, as in `inbound_frame`.
const FRAME_LIMIT: usize = 4096;
/// A stream of many tiny frames must still end.
const MAX_FRAMES: usize = 64;
/// Records per input in the sealed mode.
const MAX_RECORDS: usize = 32;

/// What happens to a sealed record on its way to the reader.
#[derive(Clone, Copy)]
enum Fate {
    Keep,
    /// One bit flipped at this offset.
    Flip(usize),
    Drop,
    /// Delivered twice.
    Replay,
    /// Cut to this length.
    Cut(usize),
}

fn fate(op: u8, record_len: usize) -> Fate {
    // The upper five bits pick an offset in 32nds of the record, so every part of it (format
    // byte, sequence number, ciphertext, tag) can be hit.
    let at = usize::from(op >> 3) * record_len / 32;
    match op & 7 {
        1 => Fate::Flip(at),
        2 => Fate::Drop,
        3 => Fate::Replay,
        4 => Fate::Cut(at),
        // Keeping is the most likely fate, so long runs of good records occur.
        _ => Fate::Keep,
    }
}

fn raw_stream(stream: &[u8]) {
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        block_on(async {
            let mut t = MessageTransport::with_codec(FuzzIo::new(stream), codec);
            t.enable_encryption(&TEST_SESSION_KEY, EndpointRole::Server);
            for _ in 0..MAX_FRAMES {
                match t
                    .recv_msg_with_codec_max::<InboundMessage>(FRAME_LIMIT)
                    .await
                {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        });
    }
}

fn sealed_records(input: &[u8]) {
    let plaintexts: Vec<(u8, &[u8])> = chunks(input, MAX_RECORDS)
        .into_iter()
        .map(|c| match c.split_first() {
            Some((&op, plain)) => (op, plain),
            None => (0, c),
        })
        .collect();

    // What arrives: (index of the sealed record, bytes, unchanged?).
    let (mut writer, _) = derive_aead_keys(&TEST_SESSION_KEY, EndpointRole::Client);
    let mut delivered: Vec<(usize, Vec<u8>, bool)> = Vec::new();
    for (i, (op, plain)) in plaintexts.iter().enumerate() {
        let mut record = writer
            .encrypt_frame(plain)
            .expect("a small record seals")
            .to_vec();
        match fate(*op, record.len()) {
            Fate::Keep => delivered.push((i, record, true)),
            Fate::Flip(at) => {
                record[at] ^= 0x01;
                delivered.push((i, record, false));
            }
            Fate::Drop => {}
            Fate::Replay => {
                delivered.push((i, record.clone(), true));
                delivered.push((i, record, true));
            }
            Fate::Cut(len) => {
                record.truncate(len);
                delivered.push((i, record, false));
            }
        }
    }

    // The record layer alone.
    let (_, mut reader) = derive_aead_keys(&TEST_SESSION_KEY, EndpointRole::Server);
    let mut next = 0usize;
    let mut open = true;
    let mut accepted = Vec::with_capacity(delivered.len());
    for (i, record, unchanged) in &delivered {
        let expected = open && *unchanged && *i == next;
        match reader.decrypt_frame(record) {
            Ok(plain) => {
                assert!(
                    expected,
                    "record {i} was accepted although it is damaged, out of order or after a refused one"
                );
                assert_eq!(
                    &plain[..],
                    plaintexts[*i].1,
                    "record {i} changed in transit"
                );
                next += 1;
            }
            Err(_) => {
                assert!(
                    !expected,
                    "record {i} arrived intact and in order but was refused"
                );
                open = false;
            }
        }
        accepted.push(expected);
    }

    // The same records through the reader half of a split transport, which decodes them.
    let stream: Vec<u8> = delivered.iter().flat_map(|(_, r, _)| frame(r)).collect();
    block_on(async {
        let mut t = MessageTransport::new(FuzzIo::new(&stream));
        t.enable_encryption(&TEST_SESSION_KEY, EndpointRole::Server);
        let (_writer, mut reader) = t.split();
        for (k, ok) in accepted.iter().enumerate() {
            match reader.recv_msg::<InboundMessage>().await {
                Ok(Some(_)) => assert!(
                    ok,
                    "record {k} decoded although the record layer refuses it"
                ),
                Ok(None) | Err(_) => break,
            }
        }
    });
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    if mode % 2 == 0 {
        raw_stream(rest);
    } else {
        sealed_records(rest);
    }
});
