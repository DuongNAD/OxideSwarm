#![no_main]
//! LV-1 target 1: bytes of a peer, read by a `MessageTransport` as `InboundMessage`, once per
//! wire codec, with a small frame limit. Covers the length prefix, the frame limit, the format
//! byte dispatch and both decoders.

use libfuzzer_sys::fuzz_target;
use rusty_grid_core::protocol::{InboundMessage, MessageTransport, WireCodec};
use rusty_grid_fuzz::{block_on, FuzzIo};

/// Small on purpose: a 64 MiB limit lets the fuzzer spend its time on allocation, not parsing.
const FRAME_LIMIT: usize = 4096;
/// A stream of many tiny frames must still end.
const MAX_FRAMES: usize = 64;

fuzz_target!(|data: &[u8]| {
    for codec in [WireCodec::Bincode, WireCodec::Json] {
        block_on(async {
            let mut t = MessageTransport::with_codec(FuzzIo::new(data), codec);
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
});
