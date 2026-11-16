#![no_main]
//! LV-1 target 2: what a peer sends first, read by the server side of the handshake with a fixed
//! test secret (v2, shared secret) or a fixed test key (v3, Noise IK).
//!
//! Input: the first byte picks the handshake (even: v2, odd: v3); the rest is the byte stream
//! of the peer, length prefixes included. On the master the HELLO version byte picks the path;
//! here both paths are driven directly so the fuzzer does not have to guess that byte.

use libfuzzer_sys::fuzz_target;
use rusty_grid_core::cluster_auth::{server_handshake, ClusterSecret};
use rusty_grid_core::noise_auth::{v3_server_handshake, NodeKeypair};
use rusty_grid_core::protocol::MessageTransport;
use rusty_grid_fuzz::{block_on, FuzzIo, TEST_MASTER_KEY, TEST_SECRET};

fuzz_target!(|data: &[u8]| {
    let Some((&mode, stream)) = data.split_first() else {
        return;
    };
    let mut t = MessageTransport::new(FuzzIo::new(stream));
    if mode % 2 == 0 {
        let secret = ClusterSecret::from_bytes(&TEST_SECRET).expect("test secret");
        let _ = block_on(server_handshake(&mut t, &secret));
    } else {
        let key = NodeKeypair::from_private_bytes(TEST_MASTER_KEY).expect("test key");
        let _ = block_on(v3_server_handshake(&mut t, &key, |_| true));
    }
});
