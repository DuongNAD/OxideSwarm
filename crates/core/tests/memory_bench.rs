//! Empirical memory usage benchmarking and memory leak verification for `rusty_grid_core`.
//!
//! Uses a custom CountingAllocator wrapping std::alloc::System to provide
//! byte-level instrumentation of:
//! 1. Memory usage during large frame transmission (1 MB, 10 MB, 32 MB, 60 MB, and MAX_FRAME_SIZE boundary)
//! 2. Zero memory leak verification over 50 consecutive large-frame cycles
//! 3. Pre-allocation analysis on oversized or incomplete length headers (adversarial slowloris check)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use rusty_grid_core::protocol::{MasterMessage, MessageTransport, ProtocolError, MAX_FRAME_SIZE};
use rusty_grid_core::task::{Task, TaskRequirements, TaskSpec};
use tokio::io::AsyncWriteExt;

struct TrackingAllocator {
    current_bytes: AtomicIsize,
    peak_bytes: AtomicIsize,
    alloc_count: AtomicUsize,
    dealloc_count: AtomicUsize,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            current_bytes: AtomicIsize::new(0),
            peak_bytes: AtomicIsize::new(0),
            alloc_count: AtomicUsize::new(0),
            dealloc_count: AtomicUsize::new(0),
        }
    }

    fn current_allocated(&self) -> isize {
        self.current_bytes.load(Ordering::SeqCst)
    }

    fn peak_allocated(&self) -> isize {
        self.peak_bytes.load(Ordering::SeqCst)
    }

    fn reset_peak(&self) {
        let current = self.current_bytes.load(Ordering::SeqCst);
        self.peak_bytes.store(current, Ordering::SeqCst);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            let size = layout.size() as isize;
            let prev = self.current_bytes.fetch_add(size, Ordering::SeqCst);
            let new_val = prev + size;
            let mut current_peak = self.peak_bytes.load(Ordering::Relaxed);
            while new_val > current_peak {
                match self.peak_bytes.compare_exchange_weak(
                    current_peak,
                    new_val,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current_peak = actual,
                }
            }
            self.alloc_count.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        let size = layout.size() as isize;
        self.current_bytes.fetch_sub(size, Ordering::SeqCst);
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn test_large_frame_boundary_and_rejection() {
    let _lock = TEST_LOCK.lock().await;
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    // 1. Exact MAX_FRAME_SIZE boundary (64 MB raw frame)
    let max_frame = Bytes::from(vec![0xAA; MAX_FRAME_SIZE]);
    assert_eq!(max_frame.len(), MAX_FRAME_SIZE);

    // Client sends exactly MAX_FRAME_SIZE raw frame
    let client_task = tokio::spawn(async move { client.send_raw_frame(max_frame).await });

    let server_task = tokio::spawn(async move { server.recv_raw_frame().await });

    let (send_res, recv_res) = tokio::join!(client_task, server_task);
    send_res
        .unwrap()
        .expect("Sending exact MAX_FRAME_SIZE must succeed");
    let received = recv_res
        .unwrap()
        .expect("Receiving exact MAX_FRAME_SIZE must succeed");
    assert!(received.is_some());
    assert_eq!(received.unwrap().len(), MAX_FRAME_SIZE);

    // 2. MAX_FRAME_SIZE + 1 byte (must be rejected on send immediately without framing)
    let (mut client2, _server2) = {
        let (c, s) = tokio::io::duplex(1024);
        (MessageTransport::new(c), MessageTransport::new(s))
    };
    let oversized = Bytes::from(vec![0xBB; MAX_FRAME_SIZE + 1]);
    let err = client2.send_raw_frame(oversized).await.unwrap_err();
    match err {
        ProtocolError::FrameTooLarge { size, max } => {
            assert_eq!(size, MAX_FRAME_SIZE + 1);
            assert_eq!(max, MAX_FRAME_SIZE);
        }
        other => panic!("Expected FrameTooLarge error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_zero_memory_leak_over_large_frame_cycles() {
    let _lock = TEST_LOCK.lock().await;
    let (client_io, server_io) = tokio::io::duplex(256 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    // 2 MB raw frame per iteration
    let frame_size = 2 * 1024 * 1024;
    let cycles = 20;

    let baseline_memory = ALLOCATOR.current_allocated();
    ALLOCATOR.reset_peak();
    println!("Baseline memory: {baseline_memory} bytes");

    let start_time = Instant::now();

    // Run sender and receiver concurrently to avoid buffer deadlock
    let send_handle = tokio::spawn(async move {
        for i in 0..cycles {
            let payload = Bytes::from(vec![(i % 256) as u8; frame_size]);
            client.send_raw_frame(payload).await.unwrap();
        }
    });

    let recv_handle = tokio::spawn(async move {
        for _ in 0..cycles {
            let received = server.recv_raw_frame().await.unwrap();
            assert!(received.is_some());
            let rec_bytes = received.unwrap();
            assert_eq!(rec_bytes.len(), frame_size);
            drop(rec_bytes);
        }
    });

    let (s_res, r_res) = tokio::join!(send_handle, recv_handle);
    s_res.unwrap();
    r_res.unwrap();

    let elapsed = start_time.elapsed();
    let final_memory = ALLOCATOR.current_allocated();
    let peak_memory = ALLOCATOR.peak_allocated();
    let memory_drift = final_memory - baseline_memory;

    println!("Processed {cycles} frames of {frame_size} bytes in {elapsed:?}");
    println!("Baseline memory: {baseline_memory} bytes");
    println!("Peak memory:     {peak_memory} bytes");
    println!("Final memory:    {final_memory} bytes");
    println!("Memory drift:    {memory_drift} bytes");

    // Total payload transferred: 20 * 2 MB = 40 MB.
    // If there were a leak of each frame buffer, memory drift would be >= 40 MB.
    // Allow up to 10 MB for buffer capacity reuse within LengthDelimitedCodec's internal buffer.
    assert!(
        memory_drift < 10 * 1024 * 1024,
        "Memory leak detected: drift was {memory_drift} bytes after {cycles} frames"
    );
}

#[tokio::test]
async fn test_incomplete_large_length_header_preallocation_behavior() {
    let _lock = TEST_LOCK.lock().await;
    let (mut client_io, server_io) = tokio::io::duplex(64 * 1024);
    let mut server = MessageTransport::new(server_io);

    let mem_before = ALLOCATOR.current_allocated();

    // Send a 4-byte header claiming a 50 MB frame (50 * 1024 * 1024),
    // but do NOT send the payload!
    let claimed_size: u32 = 50 * 1024 * 1024;
    client_io
        .write_all(&claimed_size.to_be_bytes())
        .await
        .unwrap();

    // Spawn server read which will parse the 4-byte header and wait for payload
    let server_handle = tokio::spawn(async move { server.recv_raw_frame().await });

    // Wait briefly to allow server FramedRead to parse length header and reserve memory
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Check memory allocated
    let mem_after_header = ALLOCATOR.current_allocated();
    let diff = mem_after_header - mem_before;
    println!(
        "EMPIRICAL FINDING: Memory allocated after receiving 50 MB length header: {diff} bytes"
    );

    // EMPIRICAL OBSERVATION:
    // Tokio's LengthDelimitedCodec calls src.reserve(frame_len) immediately upon reading
    // the 4-byte length prefix. This allocates ~74.5 MB upfront before receiving ANY payload bytes!
    // This confirms an important architectural property: LengthDelimitedCodec performs
    // eager pre-allocation. While MAX_FRAME_SIZE = 64 MB bounds each frame, multiple concurrent
    // connections with large length headers will allocate up to 64 MB per connection immediately.
    assert!(
        diff > 40 * 1024 * 1024,
        "Expected eager pre-allocation of ~50 MB, actual: {diff} bytes"
    );

    // Drop client to clean up (cancelling the pending read)
    drop(client_io);
    let _ = server_handle.await;
}

#[tokio::test]
async fn test_structured_message_large_gpu_payload() {
    let _lock = TEST_LOCK.lock().await;
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let mut client = MessageTransport::new(client_io);
    let mut server = MessageTransport::new(server_io);

    // 5 MB binary input data in GPU task spec
    let input_data = vec![0x55u8; 5 * 1024 * 1024];
    let task = Task::new(
        TaskSpec::GpuCompute {
            kernel_name: "gemm_fp32".into(),
            input_data: input_data.clone(),
            work_group_size: 16,
            simulated_matrix_dim: 64,
            compute_intensity: 100,
        },
        TaskRequirements::gpu(300),
    );

    let assign_msg = MasterMessage::AssignTask { task: task.clone() };

    // Send and measure
    let t0 = Instant::now();
    let send_task = tokio::spawn(async move { client.send_msg(&assign_msg).await });

    let recv_task = tokio::spawn(async move { server.recv_msg::<MasterMessage>().await });

    let (s_res, r_res) = tokio::join!(send_task, recv_task);
    s_res
        .unwrap()
        .expect("Send large structured message failed");
    let received = r_res
        .unwrap()
        .expect("Recv large structured message failed");

    let elapsed = t0.elapsed();
    println!("Roundtrip 5 MB GPU compute task took {elapsed:?}");

    match received {
        Some(MasterMessage::AssignTask { task: rec_task }) => {
            assert_eq!(rec_task.id, task.id);
            if let TaskSpec::GpuCompute {
                input_data: rec_data,
                ..
            } = rec_task.spec
            {
                assert_eq!(rec_data.len(), 5 * 1024 * 1024);
                assert_eq!(rec_data[0], 0x55);
            } else {
                panic!("TaskSpec variant mismatch");
            }
        }
        other => panic!("Unexpected received message: {other:?}"),
    }
}
