#![no_main]
//! LV-18 target 5: what `oxideswarm join` reads from another machine: a join token
//! (`oxs1-join:`), an invite (`oxs1-invite:`) and the `invite.oxs` file of a USB setup kit.
//!
//! Input: the first byte picks the mode (modulo 4).
//! - 0: the rest is the text piped into `oxideswarm join`: `read_token`, then `InviteToken::decode`
//!   or `JoinToken::decode` by its prefix, as `run_join` chooses; both decoders also read the
//!   raw text.
//! - 1: the rest is the JSON payload of a join token. The target encodes it (base64url) and adds
//!   the integrity tag made with the secret the payload names, so the fuzzer gets past the tag.
//! - 2: the rest is the JSON payload of an invite. The target adds its checksum, which the decoder
//!   checks before it parses the payload.
//! - 3: the rest is an `invite.oxs` file: `validate_invite_oxs_header` (what is read before the
//!   passphrase), then `decrypt_invite_oxs` with a fixed test passphrase when the Argon2id cost
//!   in the header is small, and the invite it holds.
//!
//! A token or invite that decodes must decode to the same value after `encode`.

use std::sync::Once;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use libfuzzer_sys::fuzz_target;
use rusty_grid_cli::join::{read_token, JoinToken, TOKEN_PREFIX};
use rusty_grid_cli::members_cmd::{InviteToken, INVITE_PREFIX};
use rusty_grid_cli::usb_kit::{
    decrypt_invite_oxs, validate_invite_oxs_header, INVITE_OXS_HEADER_LEN, INVITE_OXS_MAGIC,
};
use rusty_grid_core::cluster_auth::ClusterSecret;
use rusty_grid_core::noise_auth::invite_payload_check_hex;
use rusty_grid_fuzz::{TEST_PASSPHRASE, TEST_SECRET};

/// Largest Argon2id cost the target pays for a decryption: 256 KiB and two passes take well
/// under a millisecond. The default of a real kit (64 MiB, three passes) is only validated.
const MAX_FUZZ_M_KIB: u32 = 256;
const MAX_FUZZ_T_COST: u32 = 2;

/// The integrity tag of a join token over `payload_text`. `join.rs` keeps the label and the tag
/// length private; `check_tag_rule` stops the target if they no longer match.
fn join_tag(secret: &ClusterSecret, payload_text: &str) -> String {
    secret.derive_hex(&format!("oxs1-join-token:{payload_text}"))[..32].to_string()
}

fn check_tag_rule() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let token = JoinToken {
            master: Some("127.0.0.1:8088".into()),
            ticket: None,
            secret: token_secret(),
        }
        .encode();
        let (text, tag) = token[TOKEN_PREFIX.len()..]
            .rsplit_once('.')
            .expect("a token has a tag");
        assert_eq!(
            join_tag(&token_secret(), text),
            tag,
            "the join token tag rule changed in join.rs; update join_tag in this target"
        );
    });
}

fn token_secret() -> ClusterSecret {
    ClusterSecret::from_bytes(&TEST_SECRET).expect("test secret")
}

fn check_join(text: &str) {
    if let Ok(token) = JoinToken::decode(text) {
        let again = JoinToken::decode(&token.encode()).expect("an encoded join token decodes");
        assert_eq!(again.master, token.master);
        assert_eq!(again.ticket, token.ticket);
        assert!(again.secret == token.secret, "the secret changed");
    }
}

fn check_invite(text: &str) {
    if let Ok(invite) = InviteToken::decode(text) {
        let again = InviteToken::decode(&invite.encode()).expect("an encoded invite decodes");
        assert_eq!(again, invite);
    }
}

fn piped_text(rest: &[u8]) {
    if let Ok(text) = read_token(rest) {
        if text.starts_with(INVITE_PREFIX) || text.starts_with("oxs1-invite") {
            check_invite(&text);
        } else {
            check_join(&text);
        }
    }
    let raw = String::from_utf8_lossy(rest);
    check_join(&raw);
    check_invite(&raw);
}

fn join_payload(rest: &[u8]) {
    let text = URL_SAFE_NO_PAD.encode(rest);
    let tag = serde_json::from_slice::<serde_json::Value>(rest)
        .ok()
        .and_then(|v| v.get("s")?.as_str().map(str::to_owned))
        .and_then(|s| ClusterSecret::from_hex(&s).ok())
        .map_or_else(|| "0".repeat(32), |secret| join_tag(&secret, &text));
    check_join(&format!("{TOKEN_PREFIX}{text}.{tag}"));
}

fn invite_payload(rest: &[u8]) {
    let text = URL_SAFE_NO_PAD.encode(rest);
    let check = invite_payload_check_hex(&text);
    check_invite(&format!("{INVITE_PREFIX}{text}.{check}"));
}

fn invite_file(rest: &[u8]) {
    let Ok(header) = validate_invite_oxs_header(rest) else {
        return;
    };
    // The header is a fixed layout: written back, it must give the same bytes.
    let mut written = Vec::with_capacity(INVITE_OXS_HEADER_LEN);
    written.extend_from_slice(INVITE_OXS_MAGIC);
    written.extend_from_slice(&header.m_cost_kib.to_le_bytes());
    written.extend_from_slice(&header.t_cost.to_le_bytes());
    written.extend_from_slice(&header.p_cost.to_le_bytes());
    written.extend_from_slice(&header.salt);
    written.extend_from_slice(&header.nonce);
    assert_eq!(&rest[..INVITE_OXS_HEADER_LEN], &written[..]);

    if header.m_cost_kib <= MAX_FUZZ_M_KIB && header.t_cost <= MAX_FUZZ_T_COST {
        if let Ok(text) = decrypt_invite_oxs(rest, TEST_PASSPHRASE) {
            check_invite(&text);
        }
    }
}

fuzz_target!(|data: &[u8]| {
    check_tag_rule();
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    match mode % 4 {
        0 => piped_text(rest),
        1 => join_payload(rest),
        2 => invite_payload(rest),
        _ => invite_file(rest),
    }
});
