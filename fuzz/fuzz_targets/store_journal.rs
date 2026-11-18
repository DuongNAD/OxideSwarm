#![no_main]
//! LV-18 target 6: the readers of the master's task store (`master --state-dir`):
//! `journal.bin` (a header, then frames of one bincode `JournalRecord` each, with a BLAKE3 hash)
//! and `snapshot.bin` (a header, then one frame of a JSON `SnapshotImage`). They run through
//! `rusty_grid_master::store::fuzzing` (Cargo feature `fuzzing`), the code of `TaskStore::open`
//! on bytes in memory.
//!
//! Input: the first byte picks the mode.
//! - bit 0: 0 = journal, 1 = snapshot.
//! - bit 1: 0 = the rest is the whole file. 1 = the rest is what the frames carry, and the target
//!   writes the header, lengths and BLAKE3 hashes around it, so the fuzzer reaches the decoders
//!   behind the hashes (journal: chunks `[len: u16 LE][payload]`, one frame each; snapshot: the
//!   JSON payload).
//! - bits 2 and 3 (journal): the generation of the snapshot next to the journal, against the
//!   journal's own: equal, one newer (a journal left by an interrupted compaction), one older
//!   (refused), or 0.
//!
//! Checks: the valid prefix the journal reader reports reads back to the same records with no
//! torn tail (what `open` sees after it cut the file there); accepted records written again by
//! the store's encoder read back equal; an accepted snapshot written again reads back equal.

use libfuzzer_sys::fuzz_target;
use rusty_grid_fuzz::chunks;
use rusty_grid_master::store::fuzzing::{
    journal_frame, journal_header, journal_record_frame, read_journal, read_snapshot,
    snapshot_file, snapshot_image_file,
};
use rusty_grid_master::store::HEADER_LEN;

/// Frames per input in the framed journal mode.
const MAX_FRAMES: usize = 64;
/// Generation of the files the target writes itself.
const WRITTEN_GENERATION: u64 = 7;

fn snapshot_generation(journal_generation: u64, choice: u8) -> u64 {
    match choice & 3 {
        0 => journal_generation,
        1 => journal_generation.saturating_add(1),
        2 => journal_generation.saturating_sub(1),
        _ => 0,
    }
}

fn check_journal(file: &[u8], snapshot_gen: u64) {
    let Ok(scan) = read_journal(file, snapshot_gen) else {
        return;
    };
    if scan.reset {
        // The file is recreated; nothing of it is kept.
        return;
    }
    let valid = usize::try_from(scan.valid_len).expect("fits in usize");
    assert!(
        valid >= HEADER_LEN && valid <= file.len(),
        "valid prefix out of range"
    );
    assert_eq!(
        scan.truncated_tail,
        valid < file.len(),
        "a torn tail must be reported exactly when bytes are dropped"
    );

    // What `open` reads after it cut the file to the valid prefix.
    let cut = read_journal(&file[..valid], snapshot_gen).expect("the valid prefix reads again");
    assert!(
        !cut.truncated_tail && !cut.reset,
        "the valid prefix is not clean"
    );
    assert_eq!(cut.records, scan.records);

    if !scan.records.is_empty() {
        let mut written = journal_header(snapshot_gen).to_vec();
        for record in &scan.records {
            written.extend(journal_record_frame(record).expect("an accepted record encodes"));
        }
        let back = read_journal(&written, snapshot_gen).expect("a written journal reads again");
        assert!(!back.truncated_tail && !back.reset);
        assert_eq!(back.valid_len as usize, written.len());
        assert_eq!(back.records, scan.records);
    }
}

fn check_snapshot(file: &[u8]) {
    let Ok(scan) = read_snapshot(file) else {
        return;
    };
    let written =
        snapshot_image_file(scan.generation, &scan.image).expect("an accepted image encodes");
    let back = read_snapshot(&written).expect("a written snapshot reads again");
    assert_eq!(back.generation, scan.generation);
    assert_eq!(back.image, scan.image);
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let snapshot = mode & 1 == 1;
    let framed = mode & 2 == 2;
    match (snapshot, framed) {
        (false, false) => {
            let own = rest
                .get(16..24)
                .map_or(0, |g| u64::from_le_bytes(g.try_into().expect("8 bytes")));
            check_journal(rest, snapshot_generation(own, mode >> 2));
        }
        (false, true) => {
            let mut file = journal_header(WRITTEN_GENERATION).to_vec();
            for payload in chunks(rest, MAX_FRAMES) {
                file.extend(journal_frame(payload).expect("a small frame encodes"));
            }
            check_journal(&file, snapshot_generation(WRITTEN_GENERATION, mode >> 2));
        }
        (true, false) => check_snapshot(rest),
        (true, true) => check_snapshot(&snapshot_file(WRITTEN_GENERATION, rest)),
    }
});
