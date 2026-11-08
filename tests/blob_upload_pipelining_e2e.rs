//! Design 0039 W9: 10,000 small files go up with several blob requests in flight on one
//! connection. Kept apart from `blob_transfer_e2e` because it is slow and would compete with the
//! timing-sensitive tests there inside one process.

use std::time::Instant;

use rusty_grid_cli::blobs::{scan_inputs, upload_inputs, BlobConnection};
use rusty_grid_master::server::{MasterServer, ServerConfig};

/// A TCP relay in front of the master that counts length-prefixed frames in both directions, so a
/// test can see how many requests were in flight (sent by the client, not yet answered).
struct CountingProxy {
    addr: String,
    peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingProxy {
    async fn start(master: &str) -> Self {
        use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let peak = Arc::new(AtomicUsize::new(0));
        let master = master.to_string();
        let peak_task = Arc::clone(&peak);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = tokio::net::TcpStream::connect(&master).await.unwrap();
                let outstanding = Arc::new(AtomicIsize::new(0));
                let (mut cr, mut cw) = client.into_split();
                let (mut ur, mut uw) = upstream.into_split();
                let (out_a, peak_a) = (Arc::clone(&outstanding), Arc::clone(&peak_task));
                tokio::spawn(async move {
                    // client -> master: one frame is a 4-byte big-endian length and its payload.
                    let mut buf = vec![0u8; 1 << 20];
                    loop {
                        let mut len = [0u8; 4];
                        if cr.read_exact(&mut len).await.is_err() {
                            break;
                        }
                        let n = u32::from_be_bytes(len) as usize;
                        if buf.len() < n {
                            buf.resize(n, 0);
                        }
                        if cr.read_exact(&mut buf[..n]).await.is_err() {
                            break;
                        }
                        let now = out_a.fetch_add(1, Ordering::SeqCst) + 1;
                        peak_a.fetch_max(now.max(0) as usize, Ordering::SeqCst);
                        if uw.write_all(&len).await.is_err()
                            || uw.write_all(&buf[..n]).await.is_err()
                        {
                            break;
                        }
                    }
                });
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1 << 20];
                    loop {
                        let mut len = [0u8; 4];
                        if ur.read_exact(&mut len).await.is_err() {
                            break;
                        }
                        let n = u32::from_be_bytes(len) as usize;
                        if buf.len() < n {
                            buf.resize(n, 0);
                        }
                        if ur.read_exact(&mut buf[..n]).await.is_err() {
                            break;
                        }
                        outstanding.fetch_sub(1, Ordering::SeqCst);
                        if cw.write_all(&len).await.is_err()
                            || cw.write_all(&buf[..n]).await.is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        Self { addr, peak }
    }

    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Design 0039 W9: 10,000 small files go up with several requests in flight on one connection,
/// and the master ends up holding every one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ten_thousand_small_files_upload_with_requests_pipelined() {
    let store_dir = tempfile::tempdir().unwrap();
    let config = ServerConfig::new("127.0.0.1:0".parse().unwrap())
        .with_blob_store(store_dir.path(), 256 * 1024 * 1024);
    let master = MasterServer::spawn(config).await.expect("master starts");
    let master_addr = master.server_addr().to_string();
    let local = tempfile::tempdir().unwrap();
    let project = local.path().join("many");
    std::fs::create_dir_all(&project).unwrap();
    for i in 0..10_000u32 {
        let dir = project.join(format!("d{:02}", i % 100));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("f{i}.txt")), format!("file number {i}\n")).unwrap();
    }
    let prepared = scan_inputs(&[format!("{}:many", project.display())]).unwrap();
    assert_eq!(prepared.files, 10_000);

    let proxy = CountingProxy::start(&master_addr).await;
    let connection = BlobConnection::connect(&proxy.addr, None).await.unwrap();
    let started = Instant::now();
    let report = upload_inputs(&connection, &prepared).await.unwrap();
    eprintln!(
        "W9: uploaded {} blobs in {:?}, peak requests in flight {}",
        report.uploaded,
        started.elapsed(),
        proxy.peak()
    );
    assert_eq!(report.uploaded, report.blobs);
    assert!(
        proxy.peak() > 1,
        "uploads were strictly one request at a time"
    );
    assert!(
        proxy.peak() <= rusty_grid_core::blob_transfer::MAX_IN_FLIGHT_REQUESTS,
        "the client exceeded its in-flight cap: {}",
        proxy.peak()
    );

    // Everything arrived intact: a second pass finds nothing missing.
    let again = upload_inputs(&connection, &prepared).await.unwrap();
    assert_eq!(again.uploaded, 0);
}
