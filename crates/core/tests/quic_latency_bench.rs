//! High-precision Iroh QUIC stream RTT benchmark over GridStream.
//!
//! Measures real round-trip latency over QUIC transport:
//! - Application-level RTT over MessageTransport<GridStream::P2p>
//! - QUIC connection-level smoothed RTT via conn.paths()

#[cfg(feature = "p2p")]
#[tokio::test]
async fn benchmark_iroh_quic_stream_rtt() {
    use iroh::endpoint::RelayMode;
    use rusty_grid_core::protocol::{MasterMessage, MessageTransport, WorkerMessage};
    use rusty_grid_core::transport::{BiStream, GridStream, GRID_ALPN};
    use std::time::Instant;
    use uuid::Uuid;

    let endpoint1 = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind endpoint 1");

    let endpoint2 = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .alpns(vec![GRID_ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind endpoint 2");

    let addr1 = endpoint1.addr();

    // Server accept task spawned in background
    let ep1 = endpoint1.clone();
    let server_task = tokio::spawn(async move {
        let incoming = ep1
            .accept()
            .await
            .expect("accept incoming")
            .await
            .expect("handshake");
        let (send1, recv1) = incoming.accept_bi().await.expect("accept_bi on server");
        let stream1 = GridStream::P2p(BiStream::new(recv1, send1));
        let mut server_transport = MessageTransport::new(stream1);

        for _ in 0..120 {
            if let Ok(Some(WorkerMessage::Heartbeat { timestamp, .. })) =
                server_transport.recv_msg::<WorkerMessage>().await
            {
                let ack = MasterMessage::HeartbeatAck { timestamp };
                if server_transport.send_msg(&ack).await.is_err() {
                    break;
                }
            } else {
                break;
            }
        }
    });

    let conn2 = endpoint2
        .connect(addr1, GRID_ALPN)
        .await
        .expect("connect endpoint 2 -> 1");

    let (send2, recv2) = conn2.open_bi().await.expect("open_bi on client");
    let stream2 = GridStream::P2p(BiStream::new(recv2, send2));
    let mut client_transport = MessageTransport::new(stream2);

    let worker_id = Uuid::new_v4();

    // Warmup (10 iterations)
    for i in 0..10 {
        let hb = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: i,
            active_tasks: 0,
            cpu_usage_pct: 0.0,
            ram_available_mb: 4096,
        };
        client_transport.send_msg(&hb).await.expect("warmup send");
        let _ack: MasterMessage = client_transport
            .recv_msg()
            .await
            .expect("recv")
            .expect("ack");
    }

    // Benchmark (100 iterations)
    let sample_count = 100usize;
    let mut latencies_us = Vec::with_capacity(sample_count);

    for i in 0..sample_count {
        let hb = WorkerMessage::Heartbeat {
            worker_id,
            timestamp: 100 + i as u64,
            active_tasks: 0,
            cpu_usage_pct: 1.0,
            ram_available_mb: 4096,
        };
        let t0 = Instant::now();
        client_transport.send_msg(&hb).await.expect("bench send");
        let _ack: MasterMessage = client_transport
            .recv_msg()
            .await
            .expect("recv")
            .expect("ack");
        let elapsed = t0.elapsed();
        latencies_us.push(elapsed.as_micros() as f64);
    }

    server_task.abort();

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min_us = latencies_us[0];
    let max_us = latencies_us[sample_count - 1];
    let sum_us: f64 = latencies_us.iter().sum();
    let avg_us = sum_us / (sample_count as f64);
    let p50_us = latencies_us[(sample_count as f64 * 0.50) as usize];
    let p95_us = latencies_us[(sample_count as f64 * 0.95) as usize];
    let p99_us = latencies_us[(sample_count as f64 * 0.99) as usize];

    let quic_rtt = conn2
        .paths()
        .iter()
        .find(|p| p.is_selected())
        .map(|p| p.rtt())
        .or_else(|| conn2.paths().iter().next().map(|p| p.rtt()));

    println!("\n=== IROH QUIC STREAM (GridStream) BENCHMARK RESULTS ===");
    println!("Samples: {}", sample_count);
    println!("Min RTT: {:.3} ms ({:.1} µs)", min_us / 1000.0, min_us);
    println!("Avg RTT: {:.3} ms ({:.1} µs)", avg_us / 1000.0, avg_us);
    println!("p50 RTT: {:.3} ms ({:.1} µs)", p50_us / 1000.0, p50_us);
    println!("p95 RTT: {:.3} ms ({:.1} µs)", p95_us / 1000.0, p95_us);
    println!("p99 RTT: {:.3} ms ({:.1} µs)", p99_us / 1000.0, p99_us);
    println!("Max RTT: {:.3} ms ({:.1} µs)", max_us / 1000.0, max_us);
    if let Some(rtt) = quic_rtt {
        println!(
            "QUIC Internal Smoothed RTT: {:.3} ms",
            rtt.as_secs_f64() * 1000.0
        );
    }
    println!("======================================================\n");

    assert!(avg_us < 50_000.0, "Average QUIC RTT should be sub-50ms");
}
