//! Regression compatibility test for `invite.oxs` Argon2id key derivation (design 0053).
//!
//! Verifies that `validate_invite_oxs_header` and `decrypt_invite_oxs` reproduce the exact bytes
//! and decrypt committed `invite.oxs` test vectors generated on `argon2` `0.5.3` (multi-lane,
//! default 64 MiB parameters, and the committed `fuzz/corpus/join_invite` `0.5.3` seed).

use std::collections::BTreeMap;

use rusty_grid_cli::members_cmd::InviteToken;
use rusty_grid_cli::usb_kit::{
    decrypt_invite_oxs, validate_invite_oxs_header, INVITE_OXS_DEFAULT_M_KIB,
    INVITE_OXS_DEFAULT_P_COST, INVITE_OXS_DEFAULT_T_COST,
};
use rusty_grid_core::error::GridError;

const VEC_OXS_64_2_2: &[u8] = include_bytes!("data/invite_argon2_0_5_3.oxs");
const VEC_TXT: &str = include_str!("data/invite_argon2_0_5_3.txt");
const FUZZ_CHEAP_SEED: &[u8] =
    include_bytes!("../../../fuzz/corpus/join_invite/invite_oxs_cheap_cost");

fn parse_vectors(raw: &str) -> BTreeMap<&str, &str> {
    let mut map = BTreeMap::new();
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (key, value) = trimmed
            .split_once(' ')
            .unwrap_or_else(|| panic!("malformed vector line: {line}"));
        map.insert(key, value);
    }
    map
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0, "hex length must be even");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex byte"))
        .collect()
}

#[test]
fn decrypts_argon2_0_5_3_invite_oxs_vectors_byte_for_byte() {
    let vectors = parse_vectors(VEC_TXT);
    let passphrase = vectors["passphrase"];
    let expected_plaintext = vectors["plaintext"];

    // 1. Multi-lane 64 KiB, t=2, p=2 vector committed as both .oxs and hex.
    assert_eq!(VEC_OXS_64_2_2, decode_hex(vectors["oxs_64_2_2"]).as_slice());
    let fast_hdr = validate_invite_oxs_header(VEC_OXS_64_2_2).expect("valid 64/2/2 header");
    assert_eq!(fast_hdr.m_cost_kib, 64);
    assert_eq!(fast_hdr.t_cost, 2);
    assert_eq!(fast_hdr.p_cost, 2);
    assert_eq!(
        fast_hdr.salt,
        [
            0x10, 0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x87, 0x98, 0xa9, 0xba, 0xcb, 0xdc, 0xed,
            0xfe, 0x0f
        ]
    );
    assert_eq!(
        fast_hdr.nonce,
        [0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac]
    );

    let decrypted_fast =
        decrypt_invite_oxs(VEC_OXS_64_2_2, passphrase).expect("decrypt 0.5.3 64/2/2 vector");
    assert_eq!(&*decrypted_fast, expected_plaintext);

    // Wrong passphrase and ciphertext corruption must still fail authentication.
    let wrong_err = decrypt_invite_oxs(VEC_OXS_64_2_2, "wrong-passphrase-words")
        .expect_err("wrong passphrase must fail");
    assert!(
        matches!(wrong_err, GridError::AuthFailed(_)),
        "{wrong_err:?}"
    );

    let mut flipped = VEC_OXS_64_2_2.to_vec();
    let last = flipped.len() - 1;
    flipped[last] ^= 0x01;
    assert!(decrypt_invite_oxs(&flipped, passphrase).is_err());

    // 2. Real USB kit default cost (64 MiB = 65536 KiB, t=3, p=1) vector generated on 0.5.3.
    let oxs_default = decode_hex(vectors["oxs_65536_3_1"]);
    let def_hdr = validate_invite_oxs_header(&oxs_default).expect("valid default header");
    assert_eq!(def_hdr.m_cost_kib, INVITE_OXS_DEFAULT_M_KIB);
    assert_eq!(def_hdr.t_cost, INVITE_OXS_DEFAULT_T_COST);
    assert_eq!(def_hdr.p_cost, INVITE_OXS_DEFAULT_P_COST);
    assert_eq!(def_hdr.salt, [0x42u8; 16]);
    assert_eq!(def_hdr.nonce, [0x24u8; 12]);

    let decrypted_def =
        decrypt_invite_oxs(&oxs_default, passphrase).expect("decrypt 0.5.3 default vector");
    assert_eq!(&*decrypted_def, expected_plaintext);

    // 3. Committed fuzz seed generated with argon2 0.5.3 (mode byte 3 followed by invite.oxs).
    assert_eq!(FUZZ_CHEAP_SEED[0] % 4, 3);
    let decrypted_fuzz = decrypt_invite_oxs(&FUZZ_CHEAP_SEED[1..], "fuzz-test-passphrase")
        .expect("decrypt fuzz 0.5.3 cheap seed");
    assert!(InviteToken::decode(&decrypted_fuzz).is_ok());
}
