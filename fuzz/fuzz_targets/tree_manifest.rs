#![no_main]
//! LV-1 target 3: `TreeManifest::decode`, which reads every directory manifest that arrives over
//! the network. A manifest that decodes must survive `encode` and `decode` unchanged.

use libfuzzer_sys::fuzz_target;
use rusty_grid_core::blob::TreeManifest;

fuzz_target!(|data: &[u8]| {
    if let Ok(manifest) = TreeManifest::decode(data) {
        let again = TreeManifest::decode(&manifest.encode())
            .expect("an accepted manifest must decode again after encode");
        assert_eq!(again, manifest);
    }
});
