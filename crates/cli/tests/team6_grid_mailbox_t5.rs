//! Failing T5 acceptance tests (drafted upfront, design 0111 D8 / T5, TEAM-6 protocol 11).
//!
//! Covers:
//! 1. Send with attachment (`BlobId`), two independent consumers each fetch once, download the
//!    pinned attachment, and ack so repeat fetch sees only new messages.
//! 2. `MailWait` long poll returns promptly when matching mail arrives, and returns `timed_out`
//!    with no spurious messages on empty expiry.
//! 3. Messages, consumer ack cursors, and attachment blob pins survive master `--state-dir`
//!    restart.
//! 4. Mail byte bound evicts the oldest acknowledged mail and unpins its attachment while
//!    retaining unacknowledged mail and active attachment pins.
//! 5. Security/compat checks: rejects > 64 KiB body, missing attachment blob, and v10 peer
//!    sending v11 `Mail*` requests receives a clear version refusal.
//! 6. CLI `oxideswarm mail send --file`, `mail fetch --save`, and `mail ack` (plus symlink/junction
//!    `--save` dir refusal).

use std::time::Duration;

use rusty_grid_cli::blobs::BlobConnection;
use rusty_grid_cli::connect::connect_master;
use rusty_grid_core::blob::BlobId;
use rusty_grid_core::blob_transfer::{download_blob, upload_blob};
use rusty_grid_core::protocol::{
    serialize_message, ClientMessage, ClientResponse, MailAttachment, MessageTransport, WireCodec,
    PROTOCOL_VERSION_CURRENT,
};
use rusty_grid_master::server::{MasterHandle, MasterServer, ServerConfig};
use tokio::net::TcpStream;

const BIN: &str = env!("CARGO_BIN_EXE_oxideswarm");

async fn upload_bytes(addr: &str, bytes: &[u8]) -> BlobId {
    let ch = BlobConnection::connect(addr, None)
        .await
        .expect("connect for blob upload");
    let id = BlobId::of(bytes);
    upload_blob(&ch, id, bytes.len() as u64, std::io::Cursor::new(bytes))
        .await
        .expect("upload attachment blob");
    ch.close().await;
    id
}

async fn download_bytes(addr: &str, id: BlobId, size: u64) -> Vec<u8> {
    let ch = BlobConnection::connect(addr, None)
        .await
        .expect("connect for blob download");
    let mut out = Vec::new();
    download_blob(&ch, id, Some(size), &mut out)
        .await
        .expect("download attachment blob");
    ch.close().await;
    out
}

async fn rpc(addr: &str, msg: ClientMessage) -> ClientResponse {
    let mut transport = connect_master(addr, None).await.expect("connect for rpc");
    transport.send_msg(&msg).await.expect("send msg");
    let resp = transport
        .recv_msg::<ClientResponse>()
        .await
        .expect("recv resp")
        .expect("master response frame");
    transport.close().await;
    resp
}

async fn spawn_mailbox_master(
    blob_dir: &std::path::Path,
    state_dir: &std::path::Path,
    blob_max_bytes: u64,
    mail_max_bytes: Option<usize>,
) -> MasterHandle {
    let mut cfg = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_discovery_port(0)
        .with_blob_store(blob_dir, blob_max_bytes)
        .with_state_dir(state_dir);
    if let Some(limit) = mail_max_bytes {
        cfg = cfg.with_max_mail_bytes(limit);
    }
    MasterServer::spawn(cfg).await.expect("master starts")
}

fn create_symlink_or_junction(link: &std::path::Path, target: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link).is_ok()
    }
    #[cfg(windows)]
    {
        if std::os::windows::fs::symlink_dir(target, link).is_ok() {
            return true;
        }
        std::process::Command::new("cmd")
            .args([
                "/C",
                "mklink",
                "/J",
                link.to_str().unwrap(),
                target.to_str().unwrap(),
            ])
            .output()
            .is_ok_and(|o| o.status.success())
    }
}

#[tokio::test]
async fn t5_send_with_attachment_and_two_consumers_each_fetch_once() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let master =
        spawn_mailbox_master(blob_dir.path(), state_dir.path(), 8 * 1024 * 1024, None).await;
    let addr = master.server_addr().to_string();

    let payload = b"artifact handoff bytes for T5".to_vec();
    let blob_id = upload_bytes(&addr, &payload).await;
    let attachment = MailAttachment {
        name: "handoff.bin".to_string(),
        blob_id,
        size: payload.len() as u64,
    };

    let sent = rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["topic:team-6".to_string()],
            topic: "verify.done".to_string(),
            body: "ready for inspection".to_string(),
            attachments: vec![attachment.clone()],
        },
    )
    .await;
    let msg_id = match sent {
        ClientResponse::MailSent { id } => id,
        other => panic!("expected MailSent, got {other:?}"),
    };

    for consumer in ["agent-mac-ag", "agent-win"] {
        let fetched = rpc(
            &addr,
            ClientMessage::MailFetch {
                consumer: consumer.to_string(),
                topics: vec!["topic:team-6".to_string()],
                after: 0,
            },
        )
        .await;
        let messages = match fetched {
            ClientResponse::MailMessages { messages, .. } => messages,
            other => panic!("expected MailMessages for {consumer}, got {other:?}"),
        };
        assert_eq!(messages.len(), 1, "{consumer} should receive once");
        assert_eq!(messages[0].id, msg_id);
        assert_eq!(messages[0].attachments, vec![attachment.clone()]);

        let downloaded = download_bytes(&addr, blob_id, attachment.size).await;
        assert_eq!(downloaded, payload);

        let acked = rpc(
            &addr,
            ClientMessage::MailAck {
                consumer: consumer.to_string(),
                up_to_id: msg_id,
            },
        )
        .await;
        assert!(matches!(acked, ClientResponse::MailAcked { cursor } if cursor >= msg_id));

        let second_fetch = rpc(
            &addr,
            ClientMessage::MailFetch {
                consumer: consumer.to_string(),
                topics: vec!["topic:team-6".to_string()],
                after: 0,
            },
        )
        .await;
        match second_fetch {
            ClientResponse::MailMessages { messages, .. } => {
                assert!(
                    messages.is_empty(),
                    "{consumer} must not re-fetch acked mail"
                );
            }
            other => panic!("expected empty MailMessages after ack, got {other:?}"),
        }
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_mail_wait_wakes_promptly_on_send_and_times_out_when_empty() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let master =
        spawn_mailbox_master(blob_dir.path(), state_dir.path(), 8 * 1024 * 1024, None).await;
    let addr = master.server_addr().to_string();

    // Empty wait returns timed_out cleanly.
    let empty_wait = rpc(
        &addr,
        ClientMessage::MailWait {
            consumer: "agent-win-2".to_string(),
            topics: vec![],
            after: 0,
            timeout_secs: 1,
        },
    )
    .await;
    match empty_wait {
        ClientResponse::MailWaited {
            messages,
            timed_out,
            ..
        } => {
            assert!(messages.is_empty());
            assert!(timed_out);
        }
        other => panic!("expected MailWaited(timed_out=true), got {other:?}"),
    }

    // Spawn live long poll before sending.
    let waiter_addr = addr.clone();
    let waiter = tokio::spawn(async move {
        let mut transport = connect_master(&waiter_addr, None).await.unwrap();
        transport
            .send_msg(&ClientMessage::MailWait {
                consumer: "agent-win-2".to_string(),
                topics: vec!["topic:wake".to_string()],
                after: 0,
                timeout_secs: 5,
            })
            .await
            .unwrap();
        let resp = transport
            .recv_msg::<ClientResponse>()
            .await
            .unwrap()
            .unwrap();
        transport.close().await;
        resp
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let sent = rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["agent-win-2".to_string()],
            topic: "wake".to_string(),
            body: "wake now".to_string(),
            attachments: vec![],
        },
    )
    .await;
    let msg_id = match sent {
        ClientResponse::MailSent { id } => id,
        other => panic!("expected MailSent, got {other:?}"),
    };

    let wake_resp = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("MailWait must return within 1s of send")
        .expect("waiter task ok");
    match wake_resp {
        ClientResponse::MailWaited {
            messages,
            timed_out,
            ..
        } => {
            assert!(!timed_out);
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].body, "wake now");
        }
        other => panic!("expected MailWaited, got {other:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_mail_and_attachment_pin_survive_master_restart() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let payload = vec![b'P'; 4096];
    let filler = vec![b'F'; 24 * 1024];

    // Tight 28 KiB blob cap: filler upload later would evict unpinned attachment if pin didn't persist.
    let master1 = spawn_mailbox_master(blob_dir.path(), state_dir.path(), 28 * 1024, None).await;
    let addr1 = master1.server_addr().to_string();

    let att_id = upload_bytes(&addr1, &payload).await;
    let att = MailAttachment {
        name: "state.dat".to_string(),
        blob_id: att_id,
        size: payload.len() as u64,
    };
    let sent1 = rpc(
        &addr1,
        ClientMessage::MailSend {
            to: vec!["agent-mac-ag".to_string(), "agent-win".to_string()],
            topic: "persist".to_string(),
            body: "survives restart".to_string(),
            attachments: vec![att.clone()],
        },
    )
    .await;
    let msg_id = match sent1 {
        ClientResponse::MailSent { id } => id,
        other => panic!("expected MailSent, got {other:?}"),
    };
    let ack1 = rpc(
        &addr1,
        ClientMessage::MailAck {
            consumer: "agent-mac-ag".to_string(),
            up_to_id: msg_id,
        },
    )
    .await;
    assert!(matches!(ack1, ClientResponse::MailAcked { .. }));

    let _ = master1.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let master2 = spawn_mailbox_master(blob_dir.path(), state_dir.path(), 28 * 1024, None).await;
    let addr2 = master2.server_addr().to_string();

    // Force LRU pressure via filler blob; pinned mail attachment must survive.
    let _ = upload_bytes(&addr2, &filler).await;

    // agent-mac-ag already acked before restart -> must see empty.
    let mac_fetch = rpc(
        &addr2,
        ClientMessage::MailFetch {
            consumer: "agent-mac-ag".to_string(),
            topics: vec![],
            after: 0,
        },
    )
    .await;
    match mac_fetch {
        ClientResponse::MailMessages { messages, .. } => {
            assert!(messages.is_empty(), "acked cursor must survive restart");
        }
        other => panic!("expected empty MailMessages, got {other:?}"),
    }

    // agent-win has not acked -> must still receive mail and download pinned blob.
    let win_fetch = rpc(
        &addr2,
        ClientMessage::MailFetch {
            consumer: "agent-win".to_string(),
            topics: vec![],
            after: 0,
        },
    )
    .await;
    match win_fetch {
        ClientResponse::MailMessages { messages, .. } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
        }
        other => panic!("expected unread MailMessages after restart, got {other:?}"),
    }
    let downloaded = download_bytes(&addr2, att_id, att.size).await;
    assert_eq!(downloaded, payload);

    let _ = master2.shutdown();
}

#[tokio::test]
async fn t5_byte_bound_evicts_oldest_acknowledged_mail_and_unpins_attachment() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let payload1 = vec![b'A'; 4096];
    let master =
        spawn_mailbox_master(blob_dir.path(), state_dir.path(), 20 * 1024, Some(1024)).await;
    let addr = master.server_addr().to_string();

    let att1 = upload_bytes(&addr, &payload1).await;
    let m1 = match rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["c1".to_string()],
            topic: "evict".to_string(),
            body: "a".repeat(450),
            attachments: vec![MailAttachment {
                name: "old.bin".to_string(),
                blob_id: att1,
                size: payload1.len() as u64,
            }],
        },
    )
    .await
    {
        ClientResponse::MailSent { id } => id,
        other => panic!("{other:?}"),
    };
    let _ = rpc(
        &addr,
        ClientMessage::MailAck {
            consumer: "c1".to_string(),
            up_to_id: m1,
        },
    )
    .await;

    // m2 stays unacknowledged; m3 pushes total stored mail bytes past 1024 bytes.
    let m2 = match rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["c2".to_string()],
            topic: "keep".to_string(),
            body: "b".repeat(450),
            attachments: vec![],
        },
    )
    .await
    {
        ClientResponse::MailSent { id } => id,
        other => panic!("{other:?}"),
    };
    let m3 = match rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["c2".to_string()],
            topic: "keep".to_string(),
            body: "c".repeat(450),
            attachments: vec![],
        },
    )
    .await
    {
        ClientResponse::MailSent { id } => id,
        other => panic!("{other:?}"),
    };

    // m1 must have been evicted (even querying with a fresh consumer sees only m2, m3).
    let fetched = rpc(
        &addr,
        ClientMessage::MailFetch {
            consumer: "c2".to_string(),
            topics: vec!["evict".to_string(), "keep".to_string()],
            after: 0,
        },
    )
    .await;
    match fetched {
        ClientResponse::MailMessages { messages, .. } => {
            let ids: Vec<u64> = messages.iter().map(|m| m.id).collect();
            assert!(
                !ids.contains(&m1),
                "oldest acked mail m1 must be evicted: {ids:?}"
            );
            assert!(
                ids.contains(&m2) && ids.contains(&m3),
                "unacked m2/m3 stay: {ids:?}"
            );
        }
        other => panic!("{other:?}"),
    }

    // Upload blobs beyond 20 KiB: unpinned att1 should now be evictable without error.
    let _ = upload_bytes(&addr, &vec![b'Z'; 18 * 1024]).await;
    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_rejects_oversized_body_missing_blob_and_v10_peer_gets_clear_refusal() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let master =
        spawn_mailbox_master(blob_dir.path(), state_dir.path(), 8 * 1024 * 1024, None).await;
    let addr = master.server_addr().to_string();

    let oversized = rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["c1".to_string()],
            topic: "too-big".to_string(),
            body: "x".repeat(64 * 1024 + 1),
            attachments: vec![],
        },
    )
    .await;
    assert!(
        matches!(oversized, ClientResponse::Error { .. }),
        "{oversized:?}"
    );

    let missing_blob = rpc(
        &addr,
        ClientMessage::MailSend {
            to: vec!["c1".to_string()],
            topic: "missing".to_string(),
            body: "ok".to_string(),
            attachments: vec![MailAttachment {
                name: "ghost.bin".to_string(),
                blob_id: BlobId::of(b"not-uploaded"),
                size: 12,
            }],
        },
    )
    .await;
    assert!(
        matches!(missing_blob, ClientResponse::Error { .. }),
        "{missing_blob:?}"
    );

    // Protocol 10 peer attempting protocol 11 mailbox RPC gets an explicit version refusal.
    assert_eq!(PROTOCOL_VERSION_CURRENT, 11);
    let stream = TcpStream::connect(&addr).await.unwrap();
    let mut transport = MessageTransport::with_codec(
        rusty_grid_core::transport::GridStream::Tcp(stream),
        WireCodec::Bincode,
    );
    let mut v10_frame = serialize_message(
        &ClientMessage::MailFetch {
            consumer: "legacy".to_string(),
            topics: vec![],
            after: 0,
        },
        WireCodec::Bincode,
    )
    .unwrap()
    .to_vec();
    let len = v10_frame.len();
    v10_frame[len - 4..].copy_from_slice(&10u32.to_le_bytes());
    transport.send_raw_frame(v10_frame.into()).await.unwrap();
    let reply = transport
        .recv_msg::<ClientResponse>()
        .await
        .unwrap()
        .unwrap();
    match reply {
        ClientResponse::Error { message } => {
            assert!(
                message.contains("11") || message.to_ascii_lowercase().contains("protocol"),
                "expected clear protocol version refusal, got: {message}"
            );
        }
        other => panic!("expected ClientResponse::Error for v10 MailFetch, got {other:?}"),
    }

    let _ = master.shutdown();
}

#[tokio::test]
async fn t5_cli_mail_send_fetch_save_ack_and_refuses_symlink_save_dir() {
    let blob_dir = tempfile::tempdir().unwrap();
    let state_dir = tempfile::tempdir().unwrap();
    let master =
        spawn_mailbox_master(blob_dir.path(), state_dir.path(), 8 * 1024 * 1024, None).await;
    let addr = master.server_addr().to_string();

    let work = tempfile::tempdir().unwrap();
    let ox_home = work.path().join("ox_home");
    std::fs::create_dir_all(&ox_home).unwrap();
    let input_file = work.path().join("handoff.txt");
    std::fs::write(&input_file, b"hello via cli mail").unwrap();

    let send_out = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "send",
            "--master",
            &addr,
            "--to",
            "agent-win-2",
            "--topic",
            "cli",
            "--body",
            "cli handoff",
            "--file",
            input_file.to_str().unwrap(),
            "--json",
        ])
        .output()
        .await
        .expect("run oxideswarm mail send");
    assert!(
        send_out.status.success(),
        "mail send failed: {}",
        String::from_utf8_lossy(&send_out.stderr)
    );

    let save_dir = work.path().join("saved");
    std::fs::create_dir_all(&save_dir).unwrap();
    let fetch_out = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "fetch",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--save",
            save_dir.to_str().unwrap(),
            "--json",
        ])
        .output()
        .await
        .expect("run oxideswarm mail fetch");
    assert!(
        fetch_out.status.success(),
        "mail fetch failed: {}",
        String::from_utf8_lossy(&fetch_out.stderr)
    );
    let fetched_json: serde_json::Value = serde_json::from_slice(&fetch_out.stdout).unwrap();
    let id = fetched_json["messages"][0]["id"].as_u64().unwrap();
    assert_eq!(
        std::fs::read(save_dir.join("handoff.txt")).unwrap(),
        b"hello via cli mail"
    );

    let ack_out = tokio::process::Command::new(BIN)
        .env("OXIDESWARM_HOME", &ox_home)
        .args([
            "mail",
            "ack",
            "--master",
            &addr,
            "--consumer",
            "agent-win-2",
            "--id",
            &id.to_string(),
            "--json",
        ])
        .output()
        .await
        .expect("run oxideswarm mail ack");
    assert!(
        ack_out.status.success(),
        "mail ack failed: {}",
        String::from_utf8_lossy(&ack_out.stderr)
    );

    // Symlink/junction `--save` dir must be refused without writing anything inside target.
    let real_target = work.path().join("real_target");
    std::fs::create_dir_all(&real_target).unwrap();
    let link_save = work.path().join("link_save");
    if create_symlink_or_junction(&link_save, &real_target) {
        let symlink_fetch = tokio::process::Command::new(BIN)
            .env("OXIDESWARM_HOME", &ox_home)
            .args([
                "mail",
                "fetch",
                "--master",
                &addr,
                "--consumer",
                "agent-win-2",
                "--after",
                "0",
                "--save",
                link_save.to_str().unwrap(),
                "--json",
            ])
            .output()
            .await
            .expect("run oxideswarm mail fetch with symlink --save");
        assert!(
            !symlink_fetch.status.success(),
            "mail fetch --save pointing at symlink/junction must be refused"
        );
        let stderr = String::from_utf8_lossy(&symlink_fetch.stderr).to_ascii_lowercase();
        assert!(
            stderr.contains("symlink") || stderr.contains("link") || stderr.contains("reparse"),
            "unexpected error for symlink --save: {stderr}"
        );
        assert!(std::fs::read_dir(&real_target).unwrap().next().is_none());
    }

    let _ = master.shutdown();
}
